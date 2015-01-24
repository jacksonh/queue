//
//  EDQueue.m
//  queue
//
//  Created by Andrew Sliwinski on 6/29/12.
//  Copyright (c) 2012 Andrew Sliwinski. All rights reserved.
//

#import "EDQueue.h"
#import "EDQueueStorageEngine.h"

NSString *const EDQueueDidStart = @"EDQueueDidStart";
NSString *const EDQueueDidStop = @"EDQueueDidStop";
NSString *const EDQueueJobDidSucceed = @"EDQueueJobDidSucceed";
NSString *const EDQueueJobDidFail = @"EDQueueJobDidFail";
NSString *const EDQueueGroupDidComplete = @"EDQueueGroupDidComplete";
NSString *const EDQueueDidDrain = @"EDQueueDidDrain";

@interface EDQueue ()
{
    BOOL _isRunning;
    NSUInteger _retryLimit;
}

@property (nonatomic) EDQueueStorageEngine *engine;

@property (strong, nonatomic) NSString *currentGroup;

@property (assign, nonatomic, readonly) dispatch_queue_t processQueue;

@end


//

@implementation EDQueue

@synthesize isRunning = _isRunning;
@synthesize retryLimit = _retryLimit;

#pragma mark - Singleton

+ (EDQueue *)sharedInstance
{
    static EDQueue *singleton = nil;
    static dispatch_once_t once = 0;
    dispatch_once(&once, ^{
        singleton = [[self alloc] init];
    });
    return singleton;
}

#pragma mark - Init

- (id)init
{
    self = [super init];
    if (self) {
        _engine     = [[EDQueueStorageEngine alloc] init];
        _retryLimit = 4;
        _processQueue = dispatch_queue_create("jobProcessQueue", DISPATCH_QUEUE_CONCURRENT);
    }
    return self;
}

- (void)dealloc
{    
    self.delegate = nil;
    _engine = nil;
}

#pragma mark - Public methods

/**
 * Adds a new job to the queue.
 *
 * @param {id} Data
 * @param {NSString} Task label
 *
 * @return {void}
 */
- (void)enqueueWithData:(id)data forTask:(NSString *)task
{
    [self enqueueWithData:data priority:EDQueuePriorityDefault forTask:task];
}

- (void)enqueueWithData:(id)data priority:(EDQueuePriority)priority forTask:(NSString *)task
{
    if (data == nil) data = @{};
    [self.engine createJob:data priority:priority forTask:task inGroup:self.currentGroup];
    if (!self.currentGroup)
        [self tick];
}

/**
 * Adds a group of task to a queue, when all the jobs in the group
 * have completed, a notification will be posted. The notification
 * will contain a list of failing/succeeding tasks
 *
 * @param {id} Data
 * @param {NSString} Task label
 *
 * @return {void}
 */

- (void)enqueueGroup:(NSString *)groupName withBlock:(void (^)(EDQueue *))block
{
    NSAssert (self.currentGroup == nil, @"attempt to enqueue group from within a group block.");

    self.currentGroup = groupName;
    @try {
        block(self);
    }
    @finally {
        self.currentGroup = nil;
    }
    [self tick];
}

/**
 * Returns true if a job exists for this task.
 *
 * @param {NSString} Task label
 *
 * @return {Boolean}
 */
- (BOOL)jobExistsForTask:(NSString *)task
{
    BOOL jobExists = [self.engine jobExistsForTask:task];
    return jobExists;
}

/**
 * Returns the list of jobs for this 
 *
 * @param {NSString} Task label
 *
 * @return {NSArray}
 */
- (NSDictionary *)nextJobForTask:(NSString *)task
{
    NSDictionary *nextJobForTask = [self.engine fetchJobForTask:task];
    return nextJobForTask;
}

/**
 * Starts the queue.
 *
 * @return {void}
 */
- (void)start
{
    [self.engine promoteDeferredJobs];

    if (!self.isRunning) {
        _isRunning = YES;
        [self.engine releaseAllLocks];

        [self tick];
        [self performSelectorOnMainThread:@selector(postNotification:) withObject:[NSDictionary dictionaryWithObjectsAndKeys:EDQueueDidStart, @"name", nil, @"data", nil] waitUntilDone:false];
    }
}

/**
 * Stops the queue.
 * @note Jobs that have already started will continue to process even after stop has been called.
 *
 * @return {void}
 */
- (void)stop
{
    [self.engine releaseAllLocks];

    if (self.isRunning) {
        _isRunning = NO;
        [self performSelectorOnMainThread:@selector(postNotification:) withObject:[NSDictionary dictionaryWithObjectsAndKeys:EDQueueDidStop, @"name", nil, @"data", nil] waitUntilDone:false];
    }
}



/**
 * Empties the queue.
 * @note Jobs that have already started will continue to process even after empty has been called.
 *
 * @return {void}
 */
- (void)empty
{
    [self.engine removeAllJobs];
}


#pragma mark - Private methods

/**
 * Checks the queue for available jobs, sends them to the processor delegate, and then handles the response.
 *
 * @return {void}
 */
- (void)tick
{
    dispatch_async(self.processQueue, ^{
        if (!self.isRunning)
            return;

        id job = [self.engine fetchAndReserveJob];
        if (!job)
            return;

        // Pass job to delegate
        if ([self.delegate respondsToSelector:@selector(queue:processJob:completion:)]) {
            [self.delegate queue:self processJob:job completion:^(EDQueueResult result) {
                dispatch_sync(self.processQueue, ^{
                    [self processJob:job withResult:result];
                });
            }];
        } else {
            EDQueueResult result = [self.delegate queue:self processJob:job];
            [self processJob:job withResult:result];
        }
        NSLog (@"finished queue");
    });
}

- (void)processJob:(NSDictionary*)job withResult:(EDQueueResult)result
{
    // Check result
    switch (result) {
        case EDQueueResultSuccess:
            [self performSelectorOnMainThread:@selector(postNotification:) withObject:[NSDictionary dictionaryWithObjectsAndKeys:EDQueueJobDidSucceed, @"name", job, @"data", nil] waitUntilDone:false];
            [self.engine removeJob:[job objectForKey:@"id"]];
            break;
        case EDQueueResultDefer:
            [self.engine deferJob:job[@"id"]];
            break;
        case EDQueueResultFail:
            [self performSelectorOnMainThread:@selector(postNotification:) withObject:[NSDictionary dictionaryWithObjectsAndKeys:EDQueueJobDidFail, @"name", job, @"data", nil] waitUntilDone:true];
            NSUInteger currentAttempt = [[job objectForKey:@"attempts"] intValue] + 1;
            if (currentAttempt < self.retryLimit) {
                [self.engine incrementAttemptForJob:[job objectForKey:@"id"]];
            } else {
                [self.engine removeJob:[job objectForKey:@"id"]];
            }
            break;
        case EDQueueResultCritical:
            [self performSelectorOnMainThread:@selector(postNotification:) withObject:[NSDictionary dictionaryWithObjectsAndKeys:EDQueueJobDidFail, @"name", job, @"data", nil] waitUntilDone:false];
            [self errorWithMessage:@"Critical error. Job canceled."];
            [self.engine removeJob:[job objectForKey:@"id"]];
            break;
    }

    NSString *group = job[@"group"];
    if (group && [self.engine fetchJobCountForGroup:group] == 0) {
        [self performSelectorOnMainThread:@selector(postNotification:) withObject:@{ @"name": EDQueueGroupDidComplete, @"data": @{ @"group": group }} waitUntilDone:false];
    }

    // Drain
    if ([self.engine fetchJobCount] == 0) {
        [self performSelectorOnMainThread:@selector(postNotification:) withObject:[NSDictionary dictionaryWithObjectsAndKeys:EDQueueDidDrain, @"name", nil, @"data", nil] waitUntilDone:false];
    } else {
        [self tick];
    }
}

/**
 * Posts a notification (used to keep notifications on the main thread).
 *
 * @param {NSDictionary} Object
 *                          - name: Notification name
 *                          - data: Data to be attached to notification
 *
 * @return {void}
 */
- (void)postNotification:(NSDictionary *)object
{
    [[NSNotificationCenter defaultCenter] postNotificationName:[object objectForKey:@"name"] object:[object objectForKey:@"data"]];
}

/**
 * Writes an error message to the log.
 *
 * @param {NSString} Message
 *
 * @return {void}
 */
- (void)errorWithMessage:(NSString *)message
{
    NSLog(@"EDQueue Error: %@", message);
}

@end
