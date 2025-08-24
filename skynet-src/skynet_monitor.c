#include "skynet.h"

#include "skynet_monitor.h"
#include "skynet_server.h"
#include "skynet.h"
#include "atomic.h"

#include <stdlib.h>
#include <string.h>

struct skynet_monitor {
	ATOM_INT version;
	int check_version;
	uint32_t source;
	uint32_t destination;
};

struct skynet_monitor * 
skynet_monitor_new() {
	struct skynet_monitor * ret = skynet_malloc(sizeof(*ret));
	memset(ret, 0, sizeof(*ret));
	return ret;
}

void 
skynet_monitor_delete(struct skynet_monitor *sm) {
	skynet_free(sm);
}

// 分发消息时调用，提升版本号
void 
skynet_monitor_trigger(struct skynet_monitor *sm, uint32_t source, uint32_t destination) {
	sm->source = source;
	sm->destination = destination;
	ATOM_FINC(&sm->version);
}

// 监控线程触发，检查版本号是否自上次检查后有变化，没变化说明处理时间太久，可能发生了死锁
void 
skynet_monitor_check(struct skynet_monitor *sm) {
	if (sm->version == sm->check_version) {
		if (sm->destination) {
            // 这里会释放掉 context
			skynet_context_endless(sm->destination);
			skynet_error(NULL, "error: A message from [ :%08x ] to [ :%08x ] maybe in an endless loop (version = %d)", sm->source , sm->destination, sm->version);
		}
	} else {
		sm->check_version = sm->version;
	}
}
