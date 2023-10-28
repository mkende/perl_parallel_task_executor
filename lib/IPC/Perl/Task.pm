package IPC::Perl::Task;

use strict;
use warnings;
use utf8;

use Log::Log4perl;

my $log = Log::Log4perl->get_logger();

sub new {
  my ($class, %data) = @_;
  # %data can be anything that is needed by IPC::Perl. However the following
  # values are used by IPC::Perl::Task too:
  # - state: one of new, running, done
  # - pid: the PID of the task
  # - task_id: arbitrary identifier for the task
  # - runner: IPC::Perl runner for this task
  # - untracked: don’t count this task toward the task limit of its runner
  # - catch_error: if false, a failed task will abort the parent.
  # - channel: may be set to read the data produced by the task
  # - data: will contain the data read from the channel.
  return bless {%data}, $class;
}

sub DESTROY {
  $my ($this) = @_;
  $this->wait() if $this->running();
}

sub data {
  my ($this) = @_;
  $log->logdie("Trying to read the data of a still running task") unless $this->done();
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

sub _try_wait {
  my ($this) = @_;
  local ($!, $?);
  if ((my $pid = waitpid($this->{pid}, &WNOHANG)) > 0 ) {
    $this->_process_done();
  }
}

sub wait {
  my ($this) = @_;
  return if $this->{state} eq 'done';
  $log->logdie("Can’t wait for a task that has not yet started") if $this->{state} eq 'new';
  local ($!, $?);
  $log->logdie("No children with pid $this->{pid} for task $this->{task_id}") if waitpid($this->{pid}, &WNOHANG) == -1;
  $this->_process_done();
  return $this->{error} ? 0 : 1;
}

sub _process_done {
  my ($this) = @_;
  $this->{runner}{current_tasks}-- unless $this->{untracked};
  if ($?) {
    if ($this->{catch_error}) {
      $this->{error} = "Child command failed: $?";
    } else {
      # Ideally, we should first wait for all child processes of all runners
      # before dying, to print the dying message last.
      $log->logdie "Child process (pid == $this->{pid}, task_id == $this->{task_id}) failed";
    }
  } elsif ($this->{channel}) {
    local $/;
    my $fh = $this->{channel};
    my $data = <$fh>;
    close $fh;
    no warnings;
    no strict;
    $this->{data} = eval $data;
    $log->logdie "Cannot parse the output of child task $this->{task_id} (pid == $this->{pid}): $@" if $@;
  }
  $this->{state} = done;
  $log->trace("Child pid == $this->{pid} returned (task id == $this->{task_id}) --> current tasks == $this->{runner}{current_tasks}");
  return;
}

sub pid {
  my ($this) = @_;
  return $this->{pid};
}

1;
