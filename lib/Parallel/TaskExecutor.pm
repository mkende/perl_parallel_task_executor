package Parallel::TaskExecutor;

use strict;
use warnings;
use utf8;

use Data::Dumper;
use English;
use Exporter 'import';
use Parallel::TaskExecutor::Task;
use Log::Log4perl;
use Time::HiRes 'usleep';

our @EXPORT_OK = qw(default_executor);
our @EXPORT = @EXPORT_OK;

our @CARP_NOT = 'Parallel::TaskExecutor::Task';

our $VERSION = '0.01';

my $log = Log::Log4perl->get_logger();

=pod

=encoding utf8

=head1 NAME

Parallel::TaskExecutor

=head1 SYNOPSIS

Cross-platform executor for parallel tasks executed in forked processes.

  my $executor = Parallel::TaskExecutor->new();
  my $task = $executor->run(sub { return 'foo' });
  $task->wait();
  is($task->data(), 'foo');

=head1 DESCRIPTION

This module provides a simple interface to run Perl code in forked processes and
receive the result of their processing. This is quite similar to
L<Parallel::ForkManager> with a different OO approach, more centered on the task
object that can be seen as a very lightweight promise.

=head1 METHODS

=head2 constructor

  my $executor = Parallel::TaskExecutor->new(%options);

Create a new executor. The main possible option is:

=over 4

=item *

B<max_parallel_tasks> (default = 4): how many different sub-processes
can be created in total by this object instance.

=back

But all the options that can be passed to run() can also be passed to new() and
they will apply to all the calls to this object.

=cut

sub new {
  my ($class, %options) = @_;
  my $this =
    bless {
      max_parallel_tasks => $options{max_parallel_tasks} // 4,
      options => \%options,
      current_tasks => 0,
      zombies => [],
      pid =>$PID,
      log => $log,
    }, $class;
  return $this;
}

=pod

=head2 destructor

When a B<Parallel::TaskExecutor> goes out of scope, its destructor will wait
for all the tasks that it started and for which the returned task object is not
live. This is a complement to the destructor of L<Parallel::TaskExecutor::Task>
which waits for a task to be done if its parent executor is no longer live.

=cut

sub DESTROY {
  my ($this) = @_;
  return unless $PID == $this->{pid};
  for my $c (@{$this->{zombies}}) {
    # TODO: add an option to abandon the children (but they must be awaited by
    # someone).
    $c->wait();
  }
}

=pod

=head2 default_executor()

  my $executor = default_executor();

Returns a default B<Parallel::TaskExecutor> object with an unspecified
parallelism (guaranteed to be more than 1 parallel tasks).

=cut

my $default_executor = Parallel::TaskExecutor->new(max_parallel_tasks => 10);
sub default_executor {
  return $default_executor;
}

my $task_count = 0;

sub _fork_and_run {
  my ($this, $sub, %options) = @_;
  %options = (%{$this->{options}}, %options);
  pipe my $response_i, my $response_o;  # From the child to the parent.
  my $task_id = $task_count++;
  $this->{log}->trace("Will fork for task ${task_id}");
  my $pid = fork();
  $this->{log}->logdie("Cannot fork a sub-process") unless defined $pid;
  $this->{current_tasks}++ unless $options{untracked};

  if ($pid == 0) {
    # In the child task
    close $response_i;
    $this->{log}->trace("Starting child task (id == ${task_id}) in process ${$}");

    # $SIG{CHLD} = 'DEFAULT';
    if (exists $options{SIG}) {
      while (my ($k, $v) = each %{$options{SIG}}) {
        $SIG{$k} = $v;
      }
    }

    print $response_o "ready\n";
    $response_o->flush();
  
    my @out;
    if ($options{scalar}) {
      @out = scalar($sub->());
    } else {
      @out = $sub->();
    }
    my $serialized_out;
    {
      local $Data::Dumper::Indent = 0;
      local $Data::Dumper::Purity = 1;
      local $Data::Dumper::Sparseseen = 1;
      local $Data::Dumper::Varname = 'TASKEXECUTORVAR';
      $serialized_out = Dumper(\@out);
    }
    my $size = length($serialized_out);
    my $max_size = 4096;  # This is a very conservative estimates. On modern system the limit is 64kB.
    $this->{log}->warn(sprintf("Data returned by process ${$} for task ${task_id} is too large (%dB)", $size)) if $size > $max_size;
    # Nothing will be read before the process terminate, so the data
    print $response_o scalar($serialized_out);
    close $response_o;
    $this->{log}->trace("Exiting child task (id == ${task_id}) in process ${$}");
    exit 0;
  }

  # Still in the parent task
  $this->{log}->trace("Started child task (id == ${task_id}) with pid == ${pid}");
  close $response_o;
  my $task = Parallel::TaskExecutor::Task->new(
    untracked => $options{untracked},
    task_id => $task_id,
    runner => $this,
    state => 'running',
    channel => $response_i,
    pid => $pid,
    parent => $PID,
    catch_error => $options{catch_error},
  );

  my $ready = <$response_i>;
  die "Got unexpected data during ready check: $ready" unless $ready eq "ready\n";

  if ($options{wait}) {
    $this->{log}->trace("Waiting for child $pid to exit (task id == ${task_id})");
    $task->wait();
    $this->{log}->trace("Ok, child $pid exited (task id == ${task_id})");
  }
  return $task;
}

=pod

=head2 run()

  my $task = $executor->run($sub, %options);

Fork a new child process and use it to execute the given I<$sub>. The execution
can be tracked using the returned I<$task> object of type
L<Parallel::TaskManager::Task>.

If there are already B<max_parallel_tasks> tasks running, then the call will
block until the count of running tasks goes below that limit.

The possible options are the following:

=over 4

=item *

B<SIG> (hash-reference): if provided, this specifies a set of signal
handlers to be set in the child process. These signal handler are installed
before the provided I<$sub> is called and before the call to run() returns.

=item *

B<wait>: if set to a true value, the call to run will wait for the task
to be complete before returning (this means that C<$task->done()> will always be
true when you get the task).

=item *

B<catch_error>: by default, a failure of a child task will abort the parent
process. If this option is set to true, the failure will be reported by the task
instead.

=item *

B<scalar>: when set to true, the I<$sub> is called in scalar context. Otherwise
it is called in list context.

=item *

B<forced>: if set to true, the task will be run immediately, even if this means
exceeding the value for the B<max_parallel_tasks> passed to the constructor.
Note however that the task will still increase by one the number of running
tasks tracked by the executor (unless B<untracked> is also set to true).

=item *

B<untracked>: if set to true, the task will not increase the number of running
task counted by the executor. However, the call to run() might still be blocked
if the number of outstanding tasks exceeds B<max_parallel_tasks> (unless
B<forced> is set to true too).

=back


=cut

sub run {
  my ($this, $sub, %options) = @_;
  %options = (%{$this}, %options);
  if (!$options{forced}) {
    usleep(1000) until $this->{current_tasks} < $this->{max_parallel_tasks};
  }
  return $this->_fork_and_run($sub, %options);
}


# Same as run but does not limit the parallelism and block until the command
# has executed.
sub run_forked {
  my ($this, $sub, %options) = @_;
  $options{scalar} = 1 unless exists $options{scalar} || wantarray;
  my $task = $this->_fork_and_run($sub, %options, untracked => 1, wait => 1);
  $task->wait();
  return $task->data();
}


sub wait {
  my ($this) = @_;
  my $c = $this->{current_tasks};
  return unless $c;
  $this->{log}->debug("Waiting for ${c} running tasks...");
  usleep(1000) until $this->{current_tasks} == 0;
  #TODO: should this also execute the zombie reaping?
}

sub set_max_parallel_tasks {
  my ($this, $max_parallel_tasks) = @_;
  $this->{max_parallel_tasks} = $max_parallel_tasks;
}

1;

=pod

=head1 AUTHOR

=head1 LICENSE

=head1 SEE ALSO

=over 4

=item L<AnyEvent>

=item L<IPC::Run>

=item L<Parallel::ForkManager>

=item L<Promise::XS>

=back

=cut
