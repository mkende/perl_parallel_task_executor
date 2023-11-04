package IPC::Perl;

use strict;
use warnings;
use utf8;

use Data::Dumper;
use English;
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
      zombies => [],
      pid =>$PID,
      log => $log,
    }, $class;
  return $this;
}

sub DESTROY {
  my ($this) = @_;
  return unless $PID == $this->{pid};
  for my $c (@{$this->{zombies}}) {
    # TODO: add an option to abandon the children (but they must be awaited by
    # someone).
    $c->wait();
  }
}

my $default_runner = IPC::Perl->new();
sub default_runner {
  return $default_runner;
}

my $task_count = 0;

sub _fork_and_run {
  my ($this, $sub, %options) = @_;
  %options = (%{$this}, %options);
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
      local $Data::Dumper::Varname = 'ARDUINOBUILDERVAR';
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
  my $task = IPC::Perl::Task->new(
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
  $this->{log}->debug("Waiting for ${c} running tasks...");
  usleep(1000) until $this->{current_tasks} == 0;
}

sub set_max_parallel_tasks {
  my ($this, $max_parallel_tasks) = @_;
  $this->{max_parallel_tasks} = $max_parallel_tasks;
}

1;
