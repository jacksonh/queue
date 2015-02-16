//
//  EDQueue.h
//  queue
//
//  Created by Andrew Sliwinski on 6/29/12.
//  Copyright (c) 2012 Andrew Sliwinski. All rights reserved.
//

#import <Foundation/Foundation.h>

typedef NS_ENUM(NSInteger, EDQueueResult) {
    EDQueueResultSuccess = 0,
    EDQueueResultDefer,
    EDQueueResultFail,
    EDQueueResultCritical
};

// All high priority items are executed before default priority
typedef NS_ENUM(NSInteger, EDQueuePriority) {
    EDQueuePriorityDefault = 0,
    EDQueuePriorityHigh,
};

typedef void (^EDQueueCompletionBlock)(EDQueueResult result, NSString *response);

extern NSString *const EDQueueDidStart;
extern NSString *const EDQueueDidStop;
extern NSString *const EDQueueJobDidSucceed;
extern NSString *const EDQueueJobDidFail;
extern NSString *const EDQueueGroupDidComplete;
extern NSString *const EDQueueDidDrain;

@protocol EDQueueDelegate;
@interface EDQueue : NSObject

+ (EDQueue *)sharedInstance;

@property (nonatomic, weak) id<EDQueueDelegate> delegate;

@property (nonatomic, readonly) BOOL isRunning;
@property (nonatomic) NSUInteger retryLimit;

- (void)enqueueWithData:(id)data forTask:(NSString *)task;
- (void)enqueueWithData:(id)data priority:(EDQueuePriority)priority forTask:(NSString *)task;

- (void)enqueueGroup:(NSString *)groupName withBlock:(void (^)(EDQueue *))block;

- (void)start;
- (void)stop;
- (void)empty;

- (BOOL)jobExistsForTask:(NSString *)task;
- (NSDictionary *)nextJobForTask:(NSString *)task;

- (void)enableLogging;
- (void)disableLogging;

- (NSArray *)fetchAllJobs;

@end

@protocol EDQueueDelegate <NSObject>
@optional
- (EDQueueResult)queue:(EDQueue *)queue processJob:(NSDictionary *)job;
- (void)queue:(EDQueue *)queue processJob:(NSDictionary *)job completion:(EDQueueCompletionBlock)block;
@end
