package IPC::Perl;

use strict;
use warnings;
use utf8;

use Data::Dumper;
use Exporter 'import';
use IPC::Perl::Task;
use Log::Log4perl;
use Time::HiRes 'usleep';

our @EXPORT_OK = qw(default_runner);
our @EXPORT = @EXPORT_OK;

our @CARP_NOT = 'IPC::Perl::Task';

our $VERSION = '0.01';

my $log = Log::Log4perl->get_logger();

sub new {
  my ($class, %options) = @_;
  my $this =
    bless {
      max_parallel_tasks => $options{max_parallel_tasks} // 1,
      parallelize => $options{parallelize} // 1,
      current_tasks => 0,
      zombie => [],
    }, $class;
  return $this;
}

my $default_runner = IPC::Perl->new();
sub default_runner {
  return $default_runner;
}

my $task_count = 0;

sub _fork_and_run {
  my ($this, $sub, %options) = @_;
  %options = (%{$this}, %options);
  pipe my $tracker_i, my $tracker_o;  # From the parent to the child.
  pipe my $response_i, my $response_o;  # From the child to the parent.
  my $task_id = $task_count++;
  my $pid = fork();
  $this->{current_tasks}++ unless $options{untracked};
  $log->logdie("Cannot fork a sub-process") unless defined $pid;

  if ($pid == 0) {
    # In the child task

    # $SIG{CHLD} = 'DEFAULT';
    if (exists $options{SIG}) {
      while (my ($k, $v) = each %{$options{SIG}}) {
        $SIG{$k} = $v;
      }
      print $response_o "ready\n";
      $response_o->flush();
    }

    close $tracker_o;
    close $response_i;
    $log->trace("Starting child task (id == ${task_id}) in process ${$}");
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
      local $Data::Dumper::Varname = 'ARDUINOBUILDERVAR';
      $serialized_out = Dumper(\@out);
    }
    my $size = length($serialized_out);
    my $max_size = 4096;  # This is a very conservative estimates. On modern system the limit is 64kB.
    $log->warn(sprintf("Data returned by process ${$} for task ${task_id} is too large (%dB)", $size)) if $size > $max_size;
    # Nothing will be read before the process terminate, so the data
    print $response_o scalar(Dumper(\@out));
    # This is used to not finish the task before the children data-structure
    # was written by the parent (in which case our SIGCHLD handler could not
    # correctly track this task).
    # Ideally this should be done before running the sub, in case it never
    # returns (call exec) but, in practice it probably does not matter.
    scalar(<$tracker_i>);
    close $tracker_i;
    $log->trace("Exiting child task (id == ${task_id}) in process ${$}");
    exit 0;
  }

  # Still in the parent task
  $log->trace("Started child task (id == ${task_id}) with pid == ${pid}");
  close $tracker_i;
  close $response_o;
  my $task = IPC::Perl::Task->new(
    untracked => $options{untracked},
    task_id => $task_id,
    runner => $this,
    state => 'running',
    channel => $response_i,
    pid => $pid,
    catch_error => $options{catch_error},
  );
  if (exists $options{SIG}) {
    my $ready = <$response_i>;
    die "Got unexpected data during ready check: $ready" unless $ready eq "ready\n";
  }
  print $tracker_o "ignored\n";
  close $tracker_o;
  if ($options{wait}) {
    $log->trace("Waiting for child $pid to exit (task id == ${task_id})");
    $task->wait();
    $log->trace("Ok, child $pid exited (task id == ${task_id})");
  }
  return $task;
}

# Same as execute but does not limit the parallelism and block until the command
# has executed.
sub run_forked {
  my ($this, $sub, %options) = @_;
  $options{scalar} = 1 unless exists $options{scalar} || wantarray;
  my $task = $this->_fork_and_run($sub, %options, untracked => 1, wait => 1);
  $task->wait();
  return $task->data();
}

sub execute {
  my ($this, $sub, %options) = @_;
  %options = (%{$this}, %options);
  if (!$options{forced}) {
    usleep(1000) until $this->{current_tasks} < $this->{max_parallel_tasks};
  }
  return $this->_fork_and_run($sub, %options);
}

sub wait {
  my ($this) = @_;
  my $c = $this->{current_tasks};
  return unless $c;
  $log->debug("Waiting for ${c} running tasks...");
  usleep(1000) until $this->{current_tasks} == 0;
}

sub set_max_parallel_tasks {
  my ($this, $max_parallel_tasks) = @_;
  $this->{max_parallel_tasks} = $max_parallel_tasks;
}

1;
