#ifndef MQTTSN_H
#define MQTTSN_H
#include "Arduino.h"

enum MsgType{
	ADVERTISE 	  = 0x00,
	SEARCHGW	  = 0x01,
	GWINFO		  = 0x02,
//	reserved	  = 0x03,
	CONNECT		  = 0x04,
	CONNACK		  = 0x05,
	WILLTOPICREQ  = 0x06,
	WILLTOPICREQ  = 0x07,
	WILLMSGREQ	  = 0x08,
	WILLMSG 	  = 0x09,
	REGISTER	  = 0x0A,
	REGACK		  = 0x0B,
	PUBLISH		  = 0x0C,
	PUBACK		  = 0x0D,
	PUBCOMP		  = 0x0E,
	PUBREC		  = 0x0F,
	PUBREL		  = 0x10,
//	reserved	  = 0x11,
	SUBSCRIBE	  = 0x12,
	SUBACK		  = 0x13,
	UNSUBSCRIBE	  = 0x14,
	UNSUBACK	  = 0x15,
	PINGREQ		  = 0x16,
	PINGRESP	  = 0x17,
	DISCONNECT	  = 0x18,
//	reserved	  = 0x19,
	WILLTOPICUPD  = 0x1A,
	WILLTOPICRESP = 0x1B,
	WILLMSGUPD	  = 0x1C,
	WILLMSGRESP	  = 0x1D,
//	reserved = 0x1E-0xFD,
//	Encapsulated message = 0xFE,
//	reserved = 0xFF,
}

struct MQTTSN_Message{
	uint8_t length;
	uint8_t msgType;
}

struct MQTTSN_MessageAdvertise : MQTTSN_Message
{
	uint8_t gwId;
	uint16_t duration;
};

struct MQTTSN_MessageSearchGw : MQTTSN_Message
{
	uint8_t radius;
};

struct MQTTSN_MessageGwInfo : MQTTSN_Message
{
	uint8_t gwId;
	uint8_t gwAdd;
};

struct MQTTSN_MessageConnect : MQTTSN_Message
{
	uint8_t flags;
	uint8_t protocolId;
	uint16_t duration;
	char clientId[24];
};

struct MQTTSN_MessageConnAck : MQTTSN_Message
{
	uint8_t returnCode;
};

// struct MQTTSN_MessageWillTopicReq : MQTTSN_Message{};

// struct MQTTSN_MessageWillTopic : MQTTSN_Message
// {
// 	uint8_t flags;
// 	char willTopic[256];
// };

// struct MQTTSN_MessageWillMsgReq : MQTTSN_Message{};

// struct MQTTSN_MessageWillMsg : MQTTSN_Message
// {
// 	char willmsg[256];
// };

struct MQTTSN_MessageRegister : MQTTSN_Message
{
	uint16_t topicId;
	uint16_t msgId;
	char topicName[256];
};

struct MQTTSN_MessageRegAck : MQTTSN_Message
{
	uint16_t topicId;
	uint16_t msgId;
	uint8_t returnCode;
};

struct MQTTSN_MessagePublish : MQTTSN_Message
{
	uint8_t flags;
	uint16_t topicId;
	uint16_t msgId;
	char data[256];
};

struct MQTTSN_MessagePubAck : MQTTSN_Message
{
	uint16_t topicId;
	uint16_t msgId;
	uint8_t returnCode;
};

struct MQTTSN_MessagePub : MQTTSN_Message
{
	uint16_t msgId;
};

typedef MQTTSN_MessagePub MQTTSN_MessagePubRec;
typedef MQTTSN_MessagePub MQTTSN_MessagePubRel;
typedef MQTTSN_MessagePub MQTTSN_MessagePubComp;

struct MQTTSN_MessageSubscribe : MQTTSN_Message
{
	uint8_t flags;
	uint16_t msgId;
};

struct MQTTSN_MessageSubscribeId : MQTTSN_MessageSubscribe
{
	uint16_t topicId;
};

struct MQTTSN_MessageSubscribeName : MQTTSN_MessageSubscribe
{
	char topicName[256];
};

struct MQTTSN_MessageSubAck : MQTTSN_Message
{
	uint8_t flags;
	uint16_t topicId;
	uint16_t msgId;
	uint8_t returnCode;
};

struct MQTTSN_MessageUnsubscribeId : MQTTSN_MessageSubscribeId{};
struct MQTTSN_MessageUnsubscribeName : MQTTSN_MessageSubscribeName{};

struct MQTTSN_MessageUnsubAck : MQTTSN_Message
{
	uint16_t msgId;
};

struct MQTTSN_MessagePingReq : MQTTSN_Message
{
	char clientId[24];
};

struct MQTTSN_MessagePingResp : MQTTSN_Message{};

struct MQTTSN_MessageDisconnect : MQTTSN_Message{
	uint16_t duration;
};

// struct MQTTSN_MessageWillTopicUpd : MQTTSN_Message{
// 	uint8_t flags;
// 	char willTopic[50];
// };

// struct MQTTSN_MessageWillMsgUpd : MQTTSN_Message{
// 	uint8_t flags;
// 	char willMsg[250];
// };

// struct MQTTSN_MessageWillTopicResp : MQTTSN_Message{
// 	uint8_t returnCode;
// };

// struct MQTTSN_MessageWillMsgUpd : MQTTSN_MessageWillTopicResp{};

// struct MQTTSN_MessageEncapsulation : MQTTSN_Message
// {
// 	uint8_t ctrl;

// };

class MQTTSN
{
  public:
	MQTTSN();
	void connect(uint8_t qos, bool will, bool clean);
	uint16_t registerTopic(char topicName[]);
	bool publish(uint16_t topicId, uint8_t data[]);
	bool subscribe(char topicName[]);
	bool disconnect();
	void doWork();
  private:
	clientId[50];
};

class RF24MQTTSN : MQTTSN{
  public:
	RF24MQTTSN(RF24Network &rf);
  private:
  	RF24Network &rfnetwork;
}
#endif