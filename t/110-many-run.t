use strict;
use warnings;
use utf8;

use FindBin;
use IO::Pipe;
use Parallel::TaskExecutor;
use Test2::IPC;
use Test2::V0;

sub new {
  return Parallel::TaskExecutor->new(@_);
}

{
  pipe my $fi1, my $fo1;  # from parent to child
  pipe my $fi2, my $fo2;  # from child to parent
  my $e = new(max_parallel_tasks => 2);

  my @t;
  for (1..10) {
    push @t, $e->run(sub { return 5 });
  }
  my $r = 0;
  for my $t (@t) {
    $r += $t->get();
  }
  is ($r, 50, 'all data');
}

done_testing;
