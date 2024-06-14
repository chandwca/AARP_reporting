import unittest

if __name__ == '__main__':
    # Discover and run all tests in the 'tests' folder
    test_loader = unittest.TestLoader()
    test_suite = test_loader.discover('tests')

    test_runner = unittest.TextTestRunner(verbosity=2)
    result = test_runner.run(test_suite)

    # Print summary
    print(f"\n{'='*60}")
    print(f"Overall Test Result: {'PASSED' if result.wasSuccessful() else 'FAILED'}")
    print(f"Tests Run: {result.testsRun}")
    print(f"Failures: {len(result.failures)}")
    print(f"Errors: {len(result.errors)}")

    # Print details of each test file result
    for failed_test, traceback in result.failures + result.errors:
        print(f"\n{'-'*60}")
        print(f"Failed Test: {failed_test}")
        print(f"Traceback: {traceback}")

    if result.wasSuccessful():
        print("\nAll tests passed successfully!")
    else:
        print("\nSome tests failed. Check the details above.")
