use 5.026;

use strict;
use warnings;

use ExtUtils::MakeMaker;

WriteMakefile(
  NAME             => 'IPC::Perl',
  AUTHOR           => q{Mathias Kende <mathias@cpan.org>},
  VERSION_FROM     => 'lib/IPC/Perl.pm',
  ABSTRACT         => "Cross-platform IPC system supporting child Perl procedures",
  LICENSE          => 'mit',
  MIN_PERL_VERSION => '5.022',
  MAN3PODS         => {},
  CONFIGURE_REQUIRES => {
    'ExtUtils::MakeMaker' => '0',
  },
  PREREQ_PM => {
    'Log::Log4perl'    => '1.57',
  },
  TEST_REQUIRES => {
    'Test2::V0'             => 0,
  },
  # We don’t really need a MYMETA file, but then cpanm can’t correctly find
  # our dependencies. CPAN works correctly either way.
  # NO_MYMETA => 1,
  META_MERGE => {
    'meta-spec' => { version => 2 },
    dynamic_config => 0,
    prereqs => {
    test => {
      suggests => {
        'Perl::Tidy'               => '20220613',
        'Test::Pod'                => '1.00',
        'Test2::Tools::PerlCritic' => 0,
        },
      },
    },
    optional_features => {
      # This is set as an optional feature so that the GitHub CI does not need
      # to fetch this module that has a lot of dependencies.
      test_coverage => {
        description => 'Run `make cover` to compute test coverage',
        prereqs => {
          test => {
            requires => {
              'Devel::Cover' => 0,
            },
          },
        },
      },
    },
    keywords => [ qw(ipc open2 open3 run) ],
    resources => {
      repository => {
          type => 'git',
          url => 'git@github.com:mkende/ipc_perl.git',
          web => 'https://github.com/mkende/ipc_perl',
      },
      bugtracker => {
          web => 'https://github.com/mkende/ipc_perl/issues',
      },
    },
  },
  dist  => { COMPRESS => 'gzip -9f', SUFFIX => 'gz', },
  clean => { FILES => 'IPC-Perl-* cover_db' },
);

sub MY::postamble {
  return <<MAKE_FRAGMENT
cover:
\tcover -test

critic:
\tperlcritic lib

tidy:
\tperltidy -b lib/IPC/Perl.pm

tidydiff:
\tperltidy lib/IPC/Perl.pm
\tdiff -u lib/IPC/Perl.pm lib/IPC/Perl.tdy

spelling: export TEST_AUTHOR = 1
spelling:
\tperl -Ilib t/00-spelling.t --interactive

alltest: export TEST_AUTHOR = 1
alltest: test
MAKE_FRAGMENT
}
