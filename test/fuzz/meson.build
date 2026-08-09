if cc.get_id() != 'clang'
  error('SCRAM fuzzers require Clang with libFuzzer support')
endif

fuzz_c_args = ['-ffunction-sections', '-fsanitize=fuzzer']
fuzz_link_args = ['-fsanitize=fuzzer']
foreach flag : ['-Wl,--gc-sections', '-Wl,-dead_strip']
  if cc.has_link_argument(flag)
    fuzz_link_args += [flag]
    break
  endif
endforeach

if fuzz_link_args.length() == 1
  error('SCRAM fuzzers require linker section garbage collection support')
endif

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
