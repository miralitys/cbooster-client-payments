#import <XCTest/XCTest.h>

@interface CBoosterPaymentsUITests : XCTestCase
@end

@implementation CBoosterPaymentsUITests

- (void)setUp {
  [super setUp];
  self.continueAfterFailure = NO;
}

- (void)testAppLaunches {
  XCUIApplication *app = [[XCUIApplication alloc] init];
  [app launch];
  XCTAssertEqual(app.state, XCUIApplicationStateRunningForeground);
}

@end
