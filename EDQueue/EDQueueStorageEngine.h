//
//  EDQueueStorage.h
//  queue
//
//  Created by Andrew Sliwinski on 9/17/12.
//  Copyright (c) 2012 DIY, Co. All rights reserved.
//

#import <Foundation/Foundation.h>

@class FMDatabaseQueue;
@interface EDQueueStorageEngine : NSObject

@property (retain) FMDatabaseQueue *queue;

- (void)createJob:(id)data priority:(NSInteger)priority forTask:(id)task inGroup:(NSString *)group;
- (BOOL)jobExistsForTask:(id)task;
- (void)incrementAttemptForJob:(NSNumber *)jid;
- (void)deferJob:(NSNumber *)jid;
- (void)promoteDeferredJobs;
- (void)removeJob:(NSNumber *)jid;
- (void)removeJob:(NSNumber *)jid withResponse:(NSString *)response;
- (void)removeAllJobs;
- (NSDictionary *)fetchJob;
- (NSDictionary *)fetchAndReserveJob;
- (BOOL)reserveJob:(NSNumber *)jid;
- (NSDictionary *)fetchJobForTask:(id)task;
- (void)releaseAllLocks;

// Includes locked jobs
- (NSUInteger)fetchJobCount;
- (NSUInteger)fetchJobCountForGroup:(NSString *)group;

- (NSArray *)fetchAllJobs;

@property (assign, nonatomic) BOOL logging;

@end
