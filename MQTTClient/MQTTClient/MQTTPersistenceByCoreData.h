//
//  MQTTPersistenceByCoreData.h
//  MQTTClient
//
//  Created by Christoph Krey on 22.03.15.
//  Modified by Dmytro Hubskij
//  Copyright (c) 2015 Christoph Krey. All rights reserved.
//

#import <Foundation/Foundation.h>
#import <CoreData/CoreData.h>
#import "MQTTPersistenceProtocol.h"

@interface MQTTPersistenceByCoreData : NSObject <MQTTPersistenceProtocol>

@property (nonatomic) BOOL persistent;

@end

