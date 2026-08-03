# The '..' include dir also brings in the build root (its build-tree
# counterpart), which is where the generated test_config.h lives.
test_incdirs = include_directories('..', '../include', '../lib')

hba_test = executable('hba_test',
                      'hba_test.c',
                      '../src/hba.c',
                      '../src/util.c',
                      libusual_sources,
                      config_h,
                      build_by_default: false,
                      include_directories: test_incdirs,
                      dependencies: [libevent, openssl, systemd, threads] + net_deps,
                     )

if openssl.found()
  tls_server_end_point_test_c_args = []
  tls_server_end_point_test_link_args = []
  if cc.has_link_argument('-Wl,--wrap=X509_digest')
    tls_server_end_point_test_c_args += ['-DTEST_WRAP_X509_DIGEST']
    tls_server_end_point_test_link_args += ['-Wl,--wrap=X509_digest']
  endif
  tls_server_end_point_test = executable('tls_server_end_point_test',
                                         'tls_server_end_point_test.c',
                                         libusual_sources,
                                         config_h,
                                         build_by_default: false,
                                         c_args: tls_server_end_point_test_c_args,
                                         include_directories: test_incdirs,
                                         link_args: tls_server_end_point_test_link_args,
                                         dependencies: [openssl, systemd, threads] + net_deps,
                                        )
  test('tls_server_end_point_test',
       tls_server_end_point_test,
       workdir: meson.project_source_root() / 'test',
      )
endif

libpq = dependency('libpq', required: false, disabler: true)

asynctest = executable('asynctest',
                       'asynctest.c',
                       libusual_sources,
                       config_h,
                       build_by_default: false,
                       include_directories: test_incdirs,
                       dependencies: [libevent, libpq, systemd, threads] + net_deps,
                      )

test('hba_test',
     hba_test,
     workdir: meson.project_source_root() / 'test',
    )

if get_option('fuzzers')
  subdir('fuzz')
endif
