//
//  MQTTPersistenceProtocol.h
//  MQTTClient
//
//  Created by Dmytro Hubskyi on 11/3/15.
//  Copyright © 2015 Christoph Krey. All rights reserved.
//

#import <Foundation/Foundation.h>
#import "MQTTMessage.h"

@protocol MQTTFlow <NSObject>

@property (strong, nonatomic) NSString *clientId;
@property (assign, nonatomic) BOOL incomingFlag;
@property (assign, nonatomic) BOOL retainedFlag;
@property (assign, nonatomic) MQTTCommandType commandType;
@property (assign, nonatomic) MQTTQosLevel qosLevel;
@property (assign, nonatomic) UInt16 messageId;
@property (copy, nonatomic)   NSString *topic;
@property (copy, nonatomic)   NSData *data;
@property (copy, nonatomic)   NSDate *deadline;

@end

@protocol MQTTPersistenceProtocol <NSObject>

@property (nonatomic) NSUInteger maxWindowSize;
@property (nonatomic) NSUInteger maxMessages;
@property (nonatomic) NSUInteger maxSize;


- (NSUInteger)windowSize:(NSString *)clientId;

- (id<MQTTFlow>)storeMessageForClientId:(NSString *)clientId
                                topic:(NSString *)topic
                                 data:(NSData *)data
                           retainFlag:(BOOL)retainFlag
                                  qos:(MQTTQosLevel)qos
                                msgId:(UInt16)msgId
                         incomingFlag:(BOOL)incomingFlag;

- (void)deleteFlow:(id<MQTTFlow>)flow;

- (void)deleteAllFlowsForClientId:(NSString *)clientId;

- (NSArray <MQTTFlow> *)allFlowsforClientId:(NSString *)clientId
                    incomingFlag:(BOOL)incomingFlag;
                    
- (id<MQTTFlow>)flowforClientId:(NSString *)clientId
                 incomingFlag:(BOOL)incomingFlag
                    messageId:(UInt16)messageId;

- (id<MQTTFlow>)createFlowforClientId:(NSString *)clientId
                       incomingFlag:(BOOL)incomingFlag
                          messageId:(UInt16)messageId;
- (void)sync;


@end
