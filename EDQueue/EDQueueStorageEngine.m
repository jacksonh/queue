//
//  EDQueueStorage.m
//  queue
//
//  Created by Andrew Sliwinski on 9/17/12.
//  Copyright (c) 2012 DIY, Co. All rights reserved.
//

#import "EDQueueStorageEngine.h"

#import "FMDatabase.h"
#import "FMDatabaseAdditions.h"
#import "FMDatabasePool.h"
#import "FMDatabaseQueue.h"

@implementation EDQueueStorageEngine

#pragma mark - Init

- (id)init
{
    self = [super init];
    if (self) {
        // Database path
        NSArray *paths                  = NSSearchPathForDirectoriesInDomains(NSDocumentDirectory, NSUserDomainMask,YES);
        NSString *documentsDirectory    = [paths objectAtIndex:0];
        NSString *path                  = [documentsDirectory stringByAppendingPathComponent:@"edqueue_0.7.10d.db"];

        // Allocate the queue
        _queue                          = [[FMDatabaseQueue alloc] initWithPath:path];
        [self.queue inDatabase:^(FMDatabase *db) {
            [db executeUpdate:@"CREATE TABLE IF NOT EXISTS queue (id INTEGER PRIMARY KEY, task TEXT NOT NULL, group_name TEXT, priority INTEGER DEFAULT 0, deferred INTEGER DEFAULT 0, data TEXT NOT NULL, locked_at NUMERIC DEFAULT 0, completed_at NUMERIC DEFAULT 0, attempts INTEGER DEFAULT 0, stamp STRING DEFAULT (strftime('%s','now')) NOT NULL, udef_1 TEXT, udef_2 TEXT, response TEXT)"];
            [self _databaseHadError:[db hadError] fromDatabase:db];
            NSLog (@"db error:  %d", db.hadError);
        }];
    }

    return self;
}

- (void)dealloc
{
    _queue = nil;
}

#pragma mark - Public methods

/**
 * Creates a new job within the datastore.
 *
 * @param {NSString} Data (JSON string)
 * @param {NSString} Task name
 *
 * @return {void}
 */
- (void)createJob:(id)data priority:(NSInteger)priority forTask:(id)task inGroup:(NSString *)group
{
    NSString *dataString = [[NSString alloc] initWithData:[NSJSONSerialization dataWithJSONObject:data options:NSJSONWritingPrettyPrinted error:nil] encoding:NSUTF8StringEncoding];
    
    [self.queue inDatabase:^(FMDatabase *db) {
        [db executeUpdate:@"INSERT INTO queue (task, group_name, priority, data) VALUES (?, ?, ?, ?)", task, group,  @(priority), dataString];
        [self _databaseHadError:[db hadError] fromDatabase:db];
    }];
}

/**
 * Tells if a job exists for the specified task name.
 *
 * @param {NSString} Task name
 *
 * @return {BOOL}
 */
- (BOOL)jobExistsForTask:(id)task
{
    __block BOOL jobExists = NO;
    
    [self.queue inDatabase:^(FMDatabase *db) {
        FMResultSet *rs = [db executeQuery:@"SELECT count(id) AS count FROM queue WHERE task = ?", task];
        [self _databaseHadError:[db hadError] fromDatabase:db];
        
        while ([rs next]) {
            jobExists |= ([rs intForColumn:@"count"] > 0);
        }
        
        [rs close];
    }];
    
    return jobExists;
}

- (void)deferJob:(NSNumber *)jid
{
    [self.queue inDatabase:^(FMDatabase *db) {
        [db executeUpdate:@"UPDATE queue SET deferred = 1, locked_at = 0 WHERE id = ?", jid];
        [self _databaseHadError:[db hadError] fromDatabase:db];
    }];
}

- (void)promoteDeferredJobs
{
    [self.queue inDatabase:^(FMDatabase *db) {
        [db executeUpdate:@"UPDATE queue SET deferred = 0"];
        [self _databaseHadError:[db hadError] fromDatabase:db];
    }];
}

/**
 * Increments the "attempts" column for a specified job.
 *
 * @param {NSNumber} Job id
 *
 * @return {void}
 */
- (void)incrementAttemptForJob:(NSNumber *)jid
{
    [self.queue inDatabase:^(FMDatabase *db) {
        [db executeUpdate:@"UPDATE queue SET attempts = attempts + 1, locked_at = 0 WHERE id = ?", jid];
        [self _databaseHadError:[db hadError] fromDatabase:db];
    }];
}

/**
 * Removes a job from the datastore using a specified id.
 *
 * @param {NSNumber} Job id
 *
 * @return {void}
 */
- (void)removeJob:(NSNumber *)jid
{
    [self.queue inDatabase:^(FMDatabase *db) {
        if (self.logging) {
			[db executeUpdate:@"UPDATE queue SET completed_at = ? WHERE id = ?", @([NSDate date].timeIntervalSince1970), jid];
        } else {
            [db executeUpdate:@"DELETE FROM queue WHERE id = ?", jid];
        }
        [self _databaseHadError:[db hadError] fromDatabase:db];
    }];
}

- (void)removeJob:(NSNumber *)jid withResponse:(NSString *)response
{
	[self.queue inDatabase:^(FMDatabase *db) {
		if (self.logging) {
			[db executeUpdate:@"UPDATE queue SET completed_at = ?, response = ? WHERE id = ?", @([NSDate date].timeIntervalSince1970), response, jid];
		} else {
			[db executeUpdate:@"DELETE FROM queue WHERE id = ?", jid];
		}
		[self _databaseHadError:[db hadError] fromDatabase:db];
	}];
}

/**
 * Removes all pending jobs from the datastore
 *
 * @return {void}
 *
 */
- (void)removeAllJobs {
    [self.queue inDatabase:^(FMDatabase *db) {
        [db executeUpdate:@"DELETE FROM queue"];
        [self _databaseHadError:[db hadError] fromDatabase:db];
    }];
}

/**
 * Returns the total number of jobs within the datastore.
 *
 * @return {uint}
 */
- (NSUInteger)fetchJobCount
{
    __block NSUInteger count = 0;
    
    [self.queue inDatabase:^(FMDatabase *db) {
        FMResultSet *rs = [db executeQuery:@"SELECT count(id) AS count FROM queue WHERE deferred = 0 AND completed_at = 0"];
        [self _databaseHadError:[db hadError] fromDatabase:db];

        while ([rs next]) {
            count = [rs intForColumn:@"count"];
        }

        [rs close];
    }];
    
    return count;
}

/**
 * Returns the total number of jobs within the datastore for the supplied group.
 *
 * @return {uint}
 */
- (NSUInteger)fetchJobCountForGroup:(NSString *)group
{
    __block NSUInteger count = 0;

    [self.queue inDatabase:^(FMDatabase *db) {
        FMResultSet *rs = [db executeQuery:@"SELECT count(id) AS count FROM queue WHERE group_name = ? AND deferred = 0 AND completed_at = 0", group];
        [self _databaseHadError:[db hadError] fromDatabase:db];

        while ([rs next]) {
            count = [rs intForColumn:@"count"];
        }

        [rs close];
    }];

    return count;
}

/**
 * Returns the oldest job from the datastore.
 *
 * @return {NSDictionary}
 */
- (NSDictionary *)fetchJob
{
    __block id job;
    
    [self.queue inDatabase:^(FMDatabase *db) {
        FMResultSet *rs = [db executeQuery:@"SELECT * FROM queue WHERE locked_at = 0 AND deferred = 0 AND completed_at = 0 ORDER BY priority DESC, id ASC LIMIT 1"];
        [self _databaseHadError:[db hadError] fromDatabase:db];

        while ([rs next]) {
            job = [self _jobFromResultSet:rs];
        }
        
        [rs close];
    }];
    
    return job;
}

- (BOOL)reserveJob:(NSNumber *)jid
{
    __block BOOL rowLocked = NO;
    [self.queue inDatabase:^(FMDatabase *db) {
        [db executeUpdate:@"UPDATE queue set locked_at = ? WHERE id = ?", @([NSDate date].timeIntervalSince1970), jid];
        if (![self _databaseHadError:[db hadError] fromDatabase:db]) {
            rowLocked = (db.changes > 0);
        }
    }];

    return rowLocked;
}

- (NSDictionary *)fetchAndReserveJob
{
    __block NSDictionary *job;
    [self.queue inDatabase:^(FMDatabase *db) {
        FMResultSet *rs = [db executeQuery:@"SELECT * FROM queue WHERE locked_at = 0 AND deferred = 0 AND completed_at = 0 ORDER BY priority DESC, id ASC LIMIT 1"];
        [self _databaseHadError:[db hadError] fromDatabase:db];

        while ([rs next]) {
            job = [self _jobFromResultSet:rs];
        }

        [rs close];

        [db executeUpdate:@"UPDATE queue set locked_at = ? WHERE id = ?", @([NSDate date].timeIntervalSince1970), job[@"id"]];
        if ([self _databaseHadError:[db hadError] fromDatabase:db] || db.changes < 1) {
            job = nil;
        }
    }];

    return job;
}

- (void)releaseAllLocks
{
    [self.queue inDatabase:^(FMDatabase *db) {
        [db executeUpdate:@"UPDATE queue set locked_at = 0"];
    }];
}

/**
 * Returns the oldest job for the task from the datastore.
 *
 * @param {id} Task label
 *
 * @return {NSDictionary}
 */
- (NSDictionary *)fetchJobForTask:(id)task
{
    __block id job;
    
    [self.queue inDatabase:^(FMDatabase *db) {
        FMResultSet *rs = [db executeQuery:@"SELECT * FROM queue WHERE deferred = 0 AND task = ? AND completed_at = 0 ORDER BY priority DESC, id ASC LIMIT 1", task];
        [self _databaseHadError:[db hadError] fromDatabase:db];

        while ([rs next]) {
            job = [self _jobFromResultSet:rs];
        }

        [rs close];
    }];
    
    return job;
}

- (NSArray *)fetchAllJobs
{
	NSMutableArray *result = [[NSMutableArray alloc] init];

	[self.queue inDatabase:^(FMDatabase *db) {
		FMResultSet *rs = [db executeQuery:@"SELECT * FROM queue ORDER BY id DESC"];
		[self _databaseHadError:[db hadError] fromDatabase:db];

		while ([rs next]) {
			NSDictionary *job = [self _jobFromResultSet:rs];
			[result addObject:job];
		}

		[rs close];
	}];

	return [result copy];
}

#pragma mark - Private methods

- (NSDictionary *)_jobFromResultSet:(FMResultSet *)rs
{
    NSDictionary *job = @{
        @"id":          [NSNumber numberWithInt:[rs intForColumn:@"id"]],
        @"task":        [rs stringForColumn:@"task"],
        @"data":        [NSJSONSerialization JSONObjectWithData:[[rs stringForColumn:@"data"] dataUsingEncoding:NSUTF8StringEncoding] options:NSJSONReadingMutableContainers error:nil],
        @"priority":    @([rs intForColumn:@"priority"]),
        @"attempts":    [NSNumber numberWithInt:[rs intForColumn:@"attempts"]],
        @"stamp":			[rs stringForColumn:@"stamp"],
		@"deferred":		@([rs intForColumn:@"deferred"]),
		@"completed_at":	@([rs intForColumn:@"completed_at"]),
    };
    NSString *group = [rs stringForColumn:@"group_name"];
    if (group) {
        job = [job mutableCopy];
        [job setValue:group forKey:@"group"];
        job = [job copy];
    }
	NSString *response = [rs stringForColumn:@"response"];
	if (response) {
		job = [job mutableCopy];
		[job setValue:group forKey:@"response"];
		job = [job copy];
	}

    return job;
}

- (BOOL)_databaseHadError:(BOOL)flag fromDatabase:(FMDatabase *)db
{
	if (flag) {
		NSLog(@"Queue Database Error %d: %@", [db lastErrorCode], [db lastErrorMessage]);
	}
    return flag;
}

@end
