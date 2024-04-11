package Parallel::TaskExecutor::Task;

use strict;
use warnings;
use utf8;

use English;
use Log::Log4perl;
use POSIX ':sys_wait_h';
use Scalar::Util 'weaken';

my $log = Log::Log4perl->get_logger();

sub new {
  my ($class, %data) = @_;
  # %data can be anything that is needed by Parallel::TaskExecutor. However the
  # following values are used by Parallel::TaskExecutor::Task too:
  # - state: one of new, running, done
  # - pid: the PID of the task
  # - parent: the PID of the parent process. We don’t do anything if we’re
  #   called in a different process.
  # - task_id: arbitrary identifier for the task
  # - runner: Parallel::TaskExecutor runner for this task, kept as a weak
  #   reference
  # - untracked: don’t count this task toward the task limit of its runner
  # - catch_error: if false, a failed task will abort the parent.
  # - channel: may be set to read the data produced by the task
  # - data: will contain the data read from the channel.
  weaken($data{runner});
  return bless {%data, log => $log}, $class;
}

sub DESTROY {
  my ($this) = @_;
  return unless $PID == $this->{parent};
  # TODO: provide a system to not wait here, but defer that to the deletion of
  # the runner.
  if ($this->running()) {
    if ($this->{runner}) {
      $this->{log}->trace("Deferring reaping of task $this->{task_id}");
      push @{$this->{runner}{zombies}}, $this;
    } else {
      $this->wait();
    }
  }
}

sub data {
  my ($this) = @_;
  $this->{log}->logcroak("Trying to read the data of a still running task") unless $this->done();
  die $this->{error} if exists $this->{error};
  # TODO: we should have a variant for undef wantarray that does not setup
  # the whole pipe to get the return data.
  # Note: wantarray here is not necessarily the same as when the task was set
  # up, it is the responsibility of the caller to set the 'scalar' option
  # correctly.
  return wantarray ? @{$this->{data}} : $this->{data}[0];
}

sub running {
  my ($this) = @_;
  $this->_try_wait() if $this->{state} eq 'running';
  return $this->{state} eq 'running';
}

sub done {
  my ($this) = @_;
  $this->_try_wait() if $this->{state} eq 'running';
  return $this->{state} eq 'done';
}

sub get {
  my ($this) = @_;
  $this->wait();
  return $this->data();
}

sub _try_wait {
  my ($this) = @_;
  return if $this->{state} ne 'running';
  local ($!, $?);
  $this->{log}->trace("Starting non blocking waitpid($this->{pid})");
  if ((my $pid = waitpid($this->{pid}, WNOHANG)) > 0 ) {
    $this->_process_done();
  }
}

sub wait {
  my ($this) = @_;
  return if $this->{state} eq 'done';
  $this->{log}->logdie("Can’t wait for a task that has not yet started") if $this->{state} eq 'new';
  local ($!, $?);
  $this->{log}->trace("Starting blocking waitpid($this->{pid})");
  my $ret = waitpid($this->{pid}, 0);
  $this->{log}->logdie("No children with pid $this->{pid} for task $this->{task_id}") if $ret == -1;
  $this->{log}->logdie("Incoherent PID returned by waitpid: actual $ret; expected $this->{pid} for task $this->{task_id}") if $ret != $this->{pid};  
  $this->_process_done();
  return $this->{error} ? 0 : 1;
}

sub _process_done {
  my ($this) = @_;
  $this->{runner}{current_tasks}-- if $this->{runner} && !$this->{untracked};
  if ($?) {
    if ($this->{catch_error}) {
      $this->{error} = "Child command failed: $?";
    } else {
      # Ideally, we should first wait for all child processes of all runners
      # before dying, to print the dying message last.
      $this->{log}->logdie("Child process (pid == $this->{pid}, task_id == $this->{task_id}) failed (${?})");
    }
  } elsif ($this->{channel}) {
    local $/;
    my $fh = $this->{channel};
    my $data = <$fh>;
    close $fh;
    no warnings;
    no strict;
    $this->{data} = eval $data;
    $this->{log}->logdie("Cannot parse the output of child task $this->{task_id} (pid == $this->{pid}): $@") if $@;
  }
  $this->{state} = 'done';
  $this->{log}->trace("Child pid == $this->{pid} returned (task id == $this->{task_id})");
  $this->{log}->trace("  --> current tasks == $this->{runner}{current_tasks}") if $this->{runner};
  return;
}

sub pid {
  my ($this) = @_;
  return $this->{pid};
}

1;
