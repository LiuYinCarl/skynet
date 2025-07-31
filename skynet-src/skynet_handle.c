#include "skynet.h"

#include "skynet_handle.h"
#include "skynet_imp.h"
#include "skynet_server.h"
#include "rwlock.h"

#include <stdlib.h>
#include <assert.h>
#include <string.h>

#define DEFAULT_SLOT_SIZE 4
#define MAX_SLOT_SIZE 0x40000000

struct handle_name {
	char * name;
	uint32_t handle;
};

// 所有 handle 的存储位置
struct handle_storage {
	struct rwlock lock;

	uint32_t harbor;
	uint32_t handle_index;
	int slot_size;
	struct skynet_context ** slot;

	int name_cap;
	int name_count;
	struct handle_name *name;
};

static struct handle_storage *H = NULL;


void
print_handle_storage() {
    LLOG("=== handle_storage ===");
    LLOG("harbor: %d", H->harbor);
    LLOG("handle_index: %d", H->handle_index);
    LLOG("slot_size: %d", H->slot_size);
    for (int i = 0; i < H->slot_size; i++) {
        uint32_t hash = i & (H->slot_size-1);
        struct skynet_context * ctx = H->slot[hash];
        if (!ctx) { continue; }
        print_skynet_context(ctx);
    }
    for (int i = 0; i < H->name_count; i++) {
        struct handle_name n = H->name[i];
        LLOG("name: %s \t handle: %d", n.name, n.handle);
    }
}


// 注册一个 context,返回 handle
// 会在调用的地方将返回的 handle 设置到 ctx->handle
uint32_t
skynet_handle_register(struct skynet_context *ctx) {
	struct handle_storage *s = H;

	rwlock_wlock(&s->lock);

	for (;;) {
		int i;
		uint32_t handle = s->handle_index;
        // 从前往后，测试所有 handle
		for (i=0;i<s->slot_size;i++,handle++) {
            // handle 超过上限会绕回
			if (handle > HANDLE_MASK) {
				// 0 is reserved
				handle = 1;
			}
			int hash = handle & (s->slot_size-1);
            // 没有冲突就直接插入，否则继续找下一个可以插入的位置
            // 这里使用开放地址法解决 hash 冲突
			if (s->slot[hash] == NULL) {
				s->slot[hash] = ctx;
				s->handle_index = handle + 1;

				rwlock_wunlock(&s->lock);
                // 需要注意，在这里 handle 高8位被设置为了harbor ID
                // 之后 handle 如果需要用来计算 hash 需要去除掉高8位
				handle |= s->harbor;
				return handle;
			}
		}
		assert((s->slot_size*2 - 1) <= HANDLE_MASK);
        // 找不到任何插入位置的话，就将 slot 扩容为原来两倍
        // 并将原来 slot 中的内容迁移到新的 slot
		struct skynet_context ** new_slot = skynet_malloc(s->slot_size * 2 * sizeof(struct skynet_context *));
		memset(new_slot, 0, s->slot_size * 2 * sizeof(struct skynet_context *));
		for (i=0;i<s->slot_size;i++) {
			if (s->slot[i]) {
				int hash = skynet_context_handle(s->slot[i]) & (s->slot_size * 2 - 1);
				assert(new_slot[hash] == NULL);
				new_slot[hash] = s->slot[i];
			}
		}
		skynet_free(s->slot);
		s->slot = new_slot;
		s->slot_size *= 2;
        // 最后这里并没有将新的 ctx 插入进来
        // 这是因为这里是一个无限循环，在下一个循环重新找位置进行插入
        // 并返回，这种写法降低了逻辑复杂度
	}
}

// 取消一个 handle 的注册
int
skynet_handle_retire(uint32_t handle) {
	int ret = 0;
	struct handle_storage *s = H;

	rwlock_wlock(&s->lock);

    // 通过 handle 计算 hash
    // 通过 hash 拿到 skynet_context
	uint32_t hash = handle & (s->slot_size-1);
	struct skynet_context * ctx = s->slot[hash];

	if (ctx != NULL && skynet_context_handle(ctx) == handle) {
		s->slot[hash] = NULL;
		ret = 1;
		int i;
		int j=0, n=s->name_count;
        // 遍历所有节点，如果节点是要退休的节点，那么释放 s->name
        // 数组中对应位置的字符串，然后将s->name 数组后面的内容
        // 往前移动一个位置
		for (i=0; i<n; ++i) {
			if (s->name[i].handle == handle) {
				skynet_free(s->name[i].name);
				continue;
			} else if (i!=j) {
                //
				s->name[j] = s->name[i];
			}
			++j;
		}
		s->name_count = j;
	} else {
		ctx = NULL;
	}

	rwlock_wunlock(&s->lock);

    // 释放 skynet_context
	if (ctx) {
		// release ctx may call skynet_handle_* , so wunlock first.
		skynet_context_release(ctx);
	}

	return ret;
}

// 取消所有 handle 的注册
void
skynet_handle_retireall() {
	struct handle_storage *s = H;
	for (;;) {
		int n=0;
		int i;
		for (i=0;i<s->slot_size;i++) {
			rwlock_rlock(&s->lock);
			struct skynet_context * ctx = s->slot[i];
			uint32_t handle = 0;
			if (ctx) {
				handle = skynet_context_handle(ctx);
				++n;
			}
			rwlock_runlock(&s->lock);
			if (handle != 0) {
				skynet_handle_retire(handle);
			}
		}
		if (n==0)
			return;
	}
}

// 通过 handle 获取 skynet_context
struct skynet_context *
skynet_handle_grab(uint32_t handle) {
	struct handle_storage *s = H;
	struct skynet_context * result = NULL;

	rwlock_rlock(&s->lock);

    // 这里的 handle 高 8 位其实是 harbor ID
    // 但是因为使用了 &, 结果的高 8 位会直接变为 0，所以不需要考虑
	uint32_t hash = handle & (s->slot_size-1);
	struct skynet_context * ctx = s->slot[hash];
	if (ctx && skynet_context_handle(ctx) == handle) {
		result = ctx;
        // 增加 context 的引用计数
		skynet_context_grab(result);
	}

	rwlock_runlock(&s->lock);

	return result;
}

// 二分法查找，通过 name 找到 handle
uint32_t
skynet_handle_findname(const char * name) {
	struct handle_storage *s = H;
    // LLOG("name=%s", name); // launcher/service/cslave etc.

	rwlock_rlock(&s->lock);

	uint32_t handle = 0;

	int begin = 0;
	int end = s->name_count - 1;
	while (begin<=end) {
		int mid = (begin+end)/2;
		struct handle_name *n = &s->name[mid];
		int c = strcmp(n->name, name);
		if (c==0) {
			handle = n->handle;
			break;
		}
		if (c<0) {
			begin = mid + 1;
		} else {
			end = mid - 1;
		}
	}

	rwlock_runlock(&s->lock);

	return handle;
}

// 二分法插入，能选择这个办法应该是因为 s->name 数组中的成员变动较少
// s->name 数组按照字典序排序，所以按照 name 找 handle 可以直接二分搜索
static void
_insert_name_before(struct handle_storage *s, char *name, uint32_t handle, int before) {
	if (s->name_count >= s->name_cap) {
		s->name_cap *= 2;
		assert(s->name_cap <= MAX_SLOT_SIZE);
		struct handle_name * n = skynet_malloc(s->name_cap * sizeof(struct handle_name));
		int i;
		for (i=0;i<before;i++) {
			n[i] = s->name[i];
		}
		for (i=before;i<s->name_count;i++) {
			n[i+1] = s->name[i];
		}
		skynet_free(s->name);
		s->name = n;
	} else {
		int i;
		for (i=s->name_count;i>before;i--) {
			s->name[i] = s->name[i-1];
		}
	}
	s->name[before].name = name;
	s->name[before].handle = handle;
	s->name_count ++;
}

// 将 (name, handle) 插入到 handle_storage
// 返回一个复制后的 name 字符串的地址
static const char *
_insert_name(struct handle_storage *s, const char * name, uint32_t handle) {
	int begin = 0;
	int end = s->name_count - 1;
	while (begin<=end) {
		int mid = (begin+end)/2;
		struct handle_name *n = &s->name[mid];
		int c = strcmp(n->name, name);
		if (c==0) {
			return NULL;
		}
		if (c<0) {
			begin = mid + 1;
		} else {
			end = mid - 1;
		}
	}
    // 复制一下 name
	char * result = skynet_strdup(name);

	_insert_name_before(s, result, handle, begin);

	return result;
}

// 将 (name, handle) 信息插入到 handle_storage->name 数组
// handle_storage->name 数组按照 name 的字典序排列
const char *
skynet_handle_namehandle(uint32_t handle, const char *name) {
	rwlock_wlock(&H->lock);

	const char * ret = _insert_name(H, name, handle);

    print_handle_storage();

	rwlock_wunlock(&H->lock);

	return ret;
}

void
skynet_handle_init(int harbor) {
	assert(H==NULL);
	struct handle_storage * s = skynet_malloc(sizeof(*H));
	s->slot_size = DEFAULT_SLOT_SIZE;
	s->slot = skynet_malloc(s->slot_size * sizeof(struct skynet_context *));
	memset(s->slot, 0, s->slot_size * sizeof(struct skynet_context *));

	rwlock_init(&s->lock);
	// reserve 0 for system
	s->harbor = (uint32_t) (harbor & 0xff) << HANDLE_REMOTE_SHIFT;
	s->handle_index = 1;
	s->name_cap = 2;
	s->name_count = 0;
	s->name = skynet_malloc(s->name_cap * sizeof(struct handle_name));

	H = s;

	// Don't need to free H
}
