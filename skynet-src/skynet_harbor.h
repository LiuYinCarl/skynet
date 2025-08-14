#ifndef SKYNET_HARBOR_H
#define SKYNET_HARBOR_H

#include <stdint.h>
#include <stdlib.h>

#define GLOBALNAME_LENGTH 16
#define REMOTE_MAX 256

// 远端 harbor
struct remote_name {
	char name[GLOBALNAME_LENGTH];
	uint32_t handle;
};

// TODO 发送给远端 harbor 的消息
// 这个消息是发送给谁的？中间是否有节点负责中转
struct remote_message {
	struct remote_name destination;
	const void * message;
	size_t sz;
	int type;
};

void skynet_harbor_send(struct remote_message *rmsg, uint32_t source, int session);
int skynet_harbor_message_isremote(uint32_t handle);
void skynet_harbor_init(int harbor);
void skynet_harbor_start(void * ctx);
void skynet_harbor_exit();

#endif
