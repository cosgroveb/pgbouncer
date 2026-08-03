fuzz_c_args = ['-ffunction-sections', '-fsanitize=fuzzer']
fuzz_link_args = ['-Wl,--gc-sections', '-fsanitize=fuzzer']

executable('fuzz_scram_client_first',
           'fuzz_scram.c',
           '../../src/scram.c',
           '../../src/common/base64.c',
           config_h,
           c_args: fuzz_c_args + ['-DFUZZ_CLIENT_FIRST'],
           link_args: fuzz_link_args,
           include_directories: test_incdirs,
           dependencies: [libevent],
           build_by_default: false,
          )

executable('fuzz_scram_client_final',
           'fuzz_scram.c',
           '../../src/scram.c',
           '../../src/common/base64.c',
           config_h,
           c_args: fuzz_c_args + ['-DFUZZ_CLIENT_FINAL'],
           link_args: fuzz_link_args,
           include_directories: test_incdirs,
           dependencies: [libevent],
           build_by_default: false,
          )
