## Building tests

Use the install target to build the tests. This will ensure that all dependencies are correctly linked and that the
tests are built with the same settings as the rest of the project.

```bash
/scratch/nicholso/pelago_llvm-14/opt/bin/cmake --build /tmp/tmp.YD2SgUVlV5/cmake-build-debug --target install -- -j 80
```

## Running tests

After building, run the tests from the install directory: `/tmp/tmp.YD2SgUVlV5/cmake-build-debug/opt/pelago`

```bash
cd /tmp/tmp.YD2SgUVlV5/cmake-build-debug/opt/pelago
rm /dev/shm/*
/tmp/tmp.YD2SgUVlV5/cmake-build-debug/core/olap/tests/operator-tests/unit-operator-tests --gtest_catch_exceptions=0
```

You can run specific tests by using the `--gtest_filter` option. For example, to run the `JoinTest`:

```bash
cd /tmp/tmp.YD2SgUVlV5/cmake-build-debug/opt/pelago
rm /dev/shm/*
/tmp/tmp.YD2SgUVlV5/cmake-build-debug/core/olap/tests/operator-tests/unit-operator-tests --gtest_catch_exceptions=0 --gtest_filter=GRouterTest.default_dop_shared_random:GRouterTest/*.default_dop_shared_random:GRouterTest.default_dop_shared_random/*:*/GRouterTest.default_dop_shared_random/*:*/GRouterTest/*.default_dop_shared_random --gtest_color=no
```