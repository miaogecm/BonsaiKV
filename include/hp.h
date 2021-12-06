#ifndef HP_H
#define HP_H

#ifdef __cplusplus
extern "C" {
#endif

#define HP_K 2

typedef size_t hp_t;

/*
 * The retire nodes are organized in a list headed by hp_item->d_list.
 */
struct hp_rnode {
	hp_t address;
    struct hp_rnode* next;
} __attribute__((aligned(sizeof(long))));


struct hp_item {
	hp_t hp0;
	hp_t hp1;
	struct hp_rnode* d_list; /* first node of thread d_list */
	int d_count;   /* how many hp_rnodes in the d_list */
} __attribute__((aligned(sizeof(long))));

struct link_list;
extern struct hp_item* hp_item_setup(struct linked_list* ll, int tid);
extern void hp_setdown(struct linked_list* ll);
extern void hp_save_addr(struct hp_item* hp, int index, hp_t hp_addr);
extern void hp_clear_addr(struct hp_item* hp, int index);
extern hp_t hp_get_addr(struct hp_item* hp, int index);
extern void hp_clear_all_addr(struct hp_item* hp);
extern void hp_dump_statics(struct linked_list* ll);

extern void hp_retire_node(struct linked_list* ll, struct hp_item* hp, hp_t hp_addr);
extern void hp_retire_hp_item(struct linked_list* ll, int tid);

#ifdef __cplusplus
}
#endif

#endif
