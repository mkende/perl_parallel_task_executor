# Parallel::TaskExecutor

Cross-platform executor for parallel tasks executed in forked processes.

## Summary

This module provides a simple interface to run Perl code in forked processes and
receive the result of their processing. This is quite similar to
[Parallel::ForkManager](https://metacpan.org/pod/Parallel::ForkManager) with a
different OO approach, more centered on the task object that can be seen as a
very lightweight promise.

## Example

```perl
my $executor = Parallel::TaskExecutor->new();
my $task = $executor->run(sub { return 'foo' });
$task->wait();
is($task->data(), 'foo');
```

## Documentation

See
[this module documentation on CPAN](https://metacpan.org/pod/Parallel::ForkManager).
