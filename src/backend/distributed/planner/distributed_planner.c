/*-------------------------------------------------------------------------
 *
 * distributed_planner.c
 *	  General Citus planner code.
 *
 * Copyright (c) Citus Data, Inc.
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "distributed/pg_version_constants.h"

#include "funcapi.h"

#include <float.h>
#include <limits.h>

#include "access/htup_details.h"
#include "catalog/pg_class.h"
#include "catalog/pg_proc.h"
#include "catalog/pg_type.h"
#include "distributed/citus_nodefuncs.h"
#include "distributed/citus_nodes.h"
#include "distributed/citus_ruleutils.h"
#include "distributed/cte_inline.h"
#include "distributed/function_call_delegation.h"
#include "distributed/insert_select_planner.h"
#include "distributed/intermediate_result_pruning.h"
#include "distributed/intermediate_results.h"
#include "distributed/listutils.h"
#include "distributed/coordinator_protocol.h"
#include "distributed/metadata_cache.h"
#include "distributed/multi_executor.h"
#include "distributed/distributed_planner.h"
#include "distributed/query_pushdown_planning.h"
#include "distributed/multi_logical_optimizer.h"
#include "distributed/multi_logical_planner.h"
#include "distributed/multi_partitioning_utils.h"
#include "distributed/multi_physical_planner.h"
#include "distributed/combine_query_planner.h"
#include "distributed/multi_router_planner.h"
#include "distributed/query_utils.h"
#include "distributed/recursive_planning.h"
#include "distributed/shardinterval_utils.h"
#include "distributed/shard_utils.h"
#include "distributed/version_compat.h"
#include "distributed/worker_shard_visibility.h"
/*  danny test beign */
#include "distributed/log_utils.h"
#include "nodes/print.h"
#include <unistd.h>
/*  danny test end */
#include "executor/executor.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "nodes/pg_list.h"
#include "parser/parsetree.h"
#include "parser/parse_type.h"
#if PG_VERSION_NUM >= PG_VERSION_12
#include "optimizer/optimizer.h"
#include "optimizer/plancat.h"
#else
#include "optimizer/cost.h"
#endif
#include "optimizer/pathnode.h"
#include "optimizer/planner.h"
#include "optimizer/planmain.h"
#include "utils/builtins.h"
#include "utils/datum.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/syscache.h"


static List *plannerRestrictionContextList = NIL;
int MultiTaskQueryLogLevel = CITUS_LOG_LEVEL_OFF; /* multi-task query log level */
static uint64 NextPlanId = 1;

/* ------------- danny test begin ---------------  */
#define SECOND_TO_MILLI_SECOND 1000
#define MICRO_TO_MILLI_SECOND 0.001
static const char data_mem[] = "VmRSS:";
int DistributedPlannerRunTime = 0;
int Pid = -1;
typedef struct PerNodeUnionSubQueries
{
	char *nodeName;
	uint32 nodePort;
	uint32 nodeId;
	int  rtindex;
	List *subquerise;
} PerNodeUnionSubQueries;
//static bool RegenerateSubqueries = false;
/* ------------- danny test end ---------------  */

/* keep track of planner call stack levels */
int PlannerLevel = 0;

static bool ListContainsDistributedTableRTE(List *rangeTableList);
static bool IsUpdateOrDelete(Query *query);
static PlannedStmt * CreateDistributedPlannedStmt(
	DistributedPlanningContext *planContext);
static PlannedStmt * InlineCtesAndCreateDistributedPlannedStmt(uint64 planId,
															   DistributedPlanningContext
															   *planContext);
static PlannedStmt * TryCreateDistributedPlannedStmt(PlannedStmt *localPlan,
													 Query *originalQuery,
													 Query *query, ParamListInfo
													 boundParams,
													 PlannerRestrictionContext *
													 plannerRestrictionContext);
static DeferredErrorMessage * DeferErrorIfPartitionTableNotSingleReplicated(Oid
																			relationId);

static int AssignRTEIdentities(List *rangeTableList, int rteIdCounter);
static void AssignRTEIdentity(RangeTblEntry *rangeTableEntry, int rteIdentifier);
static void AdjustPartitioningForDistributedPlanning(List *rangeTableList,
													 bool setPartitionedTablesInherited);
static PlannedStmt * FinalizeNonRouterPlan(PlannedStmt *localPlan,
										   DistributedPlan *distributedPlan,
										   CustomScan *customScan);
static PlannedStmt * FinalizeRouterPlan(PlannedStmt *localPlan, CustomScan *customScan);
static AppendRelInfo * FindTargetAppendRelInfo(PlannerInfo *root, int relationRteIndex);
static List * makeTargetListFromCustomScanList(List *custom_scan_tlist);
static List * makeCustomScanTargetlistFromExistingTargetList(List *existingTargetlist);
static int32 BlessRecordExpressionList(List *exprs);
static void CheckNodeIsDumpable(Node *node);
static Node * CheckNodeCopyAndSerialization(Node *node);
static void AdjustReadIntermediateResultCost(RangeTblEntry *rangeTableEntry,
											 RelOptInfo *relOptInfo);
static void AdjustReadIntermediateResultArrayCost(RangeTblEntry *rangeTableEntry,
												  RelOptInfo *relOptInfo);
static void AdjustReadIntermediateResultsCostInternal(RelOptInfo *relOptInfo,
													  List *columnTypes,
													  int resultIdCount,
													  Datum *resultIds,
													  Const *resultFormatConst);
static List * OuterPlanParamsList(PlannerInfo *root);
static List * CopyPlanParamList(List *originalPlanParamList);
static PlannerRestrictionContext * CreateAndPushPlannerRestrictionContext(void);
static PlannerRestrictionContext * CurrentPlannerRestrictionContext(void);
static void PopPlannerRestrictionContext(void);
static void ResetPlannerRestrictionContext(
	PlannerRestrictionContext *plannerRestrictionContext);
static PlannedStmt * PlanFastPathDistributedStmt(DistributedPlanningContext *planContext,
												 Node *distributionKeyValue);
static PlannedStmt * PlanDistributedStmt(DistributedPlanningContext *planContext,
										 int rteIdCounter);
static RTEListProperties * GetRTEListProperties(List *rangeTableList);
static List * TranslatedVars(PlannerInfo *root, int relationIndex);
/* ------------- danny test begin ---------------  */
static void print_mem(int pid, char *func_name);
static bool IsUnionAllLinkQuery(Query *query, Node *node);
static bool IsOneLevelOrTwoLevelSelectFromSimpleViewQuery(Query *query, int *level);
static void ExpandOneLevelSimpleViewInvolvedRteList(RangeTblEntry *rte, List **rteList);
static void ExpandTwoLevelSimpleViewInvolvedRteList(RangeTblEntry *rte, List **rteList);
static void ExpandSimpleViewInvolvedTableToUpperLevel(Query *query, Node *node, List **rteList, bool *signal);
static bool RecursivelyRegenerateSubqueries(Query *query);
static bool RecursivelyRegenerateSubqueryWalker(Node *node);
static bool RecursivelyPlanSetOperationsV2Helper(Query *query, Node *node, int* nodeIds, int* signal, 
										int* nodeIdsLengh, PerNodeUnionSubQueries *perNode);
static bool RecursivelyPlanSetOperationsV2(Query *query, Node *node);
static bool RecursivelyRegenerateUnionAll(Query *query); 
static bool RecursivelyRegenerateUnionAllWalker(Node *node);

static void print_mem(int pid, char *func_name)
{
    FILE *stream;
    char cache[256];
    char mem_info[64];
    ereport(DEBUG1, (errmsg("%s", func_name)));
    sprintf(mem_info, "/proc/%d/status", pid);
    stream = fopen(mem_info, "r");
    if (stream == NULL) {
        return;
    }

    while(fscanf(stream, "%s", cache) != EOF) {
        if (strncmp(cache, data_mem, sizeof(data_mem)) == 0) {
            if (fscanf(stream, "%s", cache) != EOF) {
                ereport(DEBUG1, (errmsg("hw memory[%s]<=======", cache)));
                break;
            }
        } 
    }
    fclose(stream); 
    return;
}

static bool IsUnionAllLinkQuery(Query *query, Node *node) {
	if (IsA(node, SetOperationStmt)) {
		SetOperationStmt *setOperations = (SetOperationStmt *) node;
		if (setOperations->op != SETOP_UNION || setOperations->all != true) {
			return false;
		}
		bool sign = IsUnionAllLinkQuery(query, setOperations->larg);
		if (sign == false) {
			return sign;
		}
		sign = IsUnionAllLinkQuery(query, setOperations->rarg);
		if (sign == false) {
			return sign;
		}
	} else if (IsA(node, RangeTblRef)) {
		RangeTblRef *rangeTableRef = (RangeTblRef *) node;
		RangeTblEntry *rangeTableEntry = rt_fetch(rangeTableRef->rtindex,
												  query->rtable);
		if (rangeTableEntry->rtekind != RTE_SUBQUERY) {
			return false;
		}
	} else {
		ereport(ERROR, (errmsg("unexpected node type (%d) while "
							   "expecting set operations or "
							   "range table references", nodeTag(node))));
	}
	return true;
} 

/* SimpleViewQuery defination: some tables linked by union all, and each query not have join
 * SimpleViewQuery defination eg: select xxx from tableA where yyy union all select xxx from tableB where yyy
 * OneLevel:
 * eg: select xxx from view where yyy
 * TwoLevel:
 * eg: select xxx from (select sss from view where yyy) where zzz
 * condition sss only allow window func row_number()
*/
static bool IsOneLevelOrTwoLevelSelectFromSimpleViewQuery(Query *query, int *level) {
	if (query->commandType == CMD_SELECT && query->hasWindowFuncs == false && query->hasTargetSRFs == false && query->hasSubLinks == false
		&& query->hasDistinctOn == false && query->hasRecursive == false && query->hasModifyingCTE == false
		&& query->hasForUpdate == false && query->hasRowSecurity == false && query->cteList == NULL && list_length(query->rtable) == 1
		&& query->jointree != NULL && list_length(query->jointree->fromlist) == 1 && list_length(query->groupClause) == 0 
		&& list_length(query->groupingSets) == 0 && list_length(query->distinctClause) == 0) {
		RangeTblEntry *rte = (RangeTblEntry *) lfirst(list_head(query->rtable));
		if (rte->rtekind == RTE_SUBQUERY) {  
			Query* subquery = rte->subquery;
			if (subquery->hasWindowFuncs == false) { // check if satisfy OneLevel condition 
				// subquery don't have from and where statement, only have union all
				if (subquery->commandType == CMD_SELECT && subquery->hasAggs == false && subquery->hasTargetSRFs == false
					&& subquery->hasSubLinks == false && subquery->hasDistinctOn == false && subquery->hasRecursive == false 
					&& subquery->hasModifyingCTE == false && subquery->hasForUpdate == false && subquery->hasRowSecurity == false 
					&& subquery->cteList == NULL && subquery->jointree != NULL && list_length(subquery->jointree->fromlist) == 0 
					&& subquery->jointree->quals == NULL && subquery->setOperations != NULL
					&& IsUnionAllLinkQuery(subquery, (Node *) subquery->setOperations)) {
					// Check whether the definition of SimpleViewQuery is met
					bool allPhyisicalTableFollowRules = true;
					ListCell *lc;
					foreach(lc, subquery->rtable){
						RangeTblEntry *srte = (RangeTblEntry *) lfirst(lc);
						if (srte->rtekind == RTE_SUBQUERY) {
							Query* ssubquery = srte->subquery;
					
							if (ssubquery->commandType == CMD_SELECT && ssubquery->hasAggs == false && ssubquery->hasWindowFuncs == false
								&& ssubquery->hasTargetSRFs == false && ssubquery->hasSubLinks == false && ssubquery->hasDistinctOn == false
								&& ssubquery->hasRecursive == false && ssubquery->hasModifyingCTE == false && ssubquery->hasForUpdate == false
								&& ssubquery->hasRowSecurity == false && ssubquery->cteList == NULL && list_length(ssubquery->rtable) == 1 
								&& ssubquery->jointree != NULL && list_length(ssubquery->jointree->fromlist) == 1 && ssubquery->setOperations == NULL) {
								RangeTblEntry *ssrte = (RangeTblEntry *) lfirst(list_head(ssubquery->rtable));
								if (ssrte->rtekind != RTE_RELATION || ssrte->relkind != 'r') {
									allPhyisicalTableFollowRules = false;
									break;
								}
							} else {
								allPhyisicalTableFollowRules = false;
								break;
							}
						} 
					}
					if (allPhyisicalTableFollowRules) {
						*level = 1;
						return true;
					} 
				}
				// check if satisfy TwoLevel condition 
				// subquery don't have from and where statement, only have union
				if (subquery->commandType == CMD_SELECT && subquery->hasTargetSRFs == false && subquery->hasSubLinks == false
					&& subquery->hasDistinctOn == false && subquery->hasRecursive == false && subquery->hasModifyingCTE == false
					&& subquery->hasForUpdate == false && subquery->hasRowSecurity == false && subquery->cteList == NULL 
					&& list_length(subquery->rtable) == 1 && subquery->jointree != NULL && list_length(subquery->jointree->fromlist) == 1) {
					RangeTblEntry *rrte = (RangeTblEntry *) lfirst(list_head(subquery->rtable)); 
					if (rrte->rtekind == RTE_SUBQUERY) {
						// bool secondLevelQueryHasWhere = false;
						// if (subquery->jointree->quals != NULL) {
						// 	secondLevelQueryHasWhere = true;
						// }
						Query* ssubquery = rrte->subquery; // get second level subquery
						// make sure this second level subquery is generate by multi table union on (without join or other complex query)
						if (ssubquery != NULL && ssubquery->commandType == CMD_SELECT && ssubquery->hasAggs == false && ssubquery->hasTargetSRFs == false 
							&& ssubquery->hasSubLinks == false && ssubquery->hasDistinctOn == false && ssubquery->hasRecursive == false
							&& ssubquery->hasModifyingCTE == false && ssubquery->hasForUpdate == false&& ssubquery->hasRowSecurity == false 
							&& ssubquery->cteList == NULL && ssubquery->jointree != NULL && list_length(ssubquery->jointree->fromlist) == 0 
							&& ssubquery->jointree->quals == NULL && ssubquery->setOperations != NULL && ssubquery->hasWindowFuncs == false) {
							bool allPhyisicalTableFollowRules = true;
							ListCell *lc;
							// union on involved tables can not has join or other complex query
							foreach(lc, ssubquery->rtable){ 
								RangeTblEntry *srte = (RangeTblEntry *) lfirst(lc);
								if (srte->rtekind == RTE_SUBQUERY) {
									Query* sssubquery = srte->subquery;
			
									if (sssubquery->commandType == CMD_SELECT && sssubquery->hasAggs == false && sssubquery->hasWindowFuncs == false
										&& sssubquery->hasTargetSRFs == false && sssubquery->hasSubLinks == false && sssubquery->hasDistinctOn == false
										&& sssubquery->hasRecursive == false && sssubquery->hasModifyingCTE == false && sssubquery->hasForUpdate == false
										&& sssubquery->hasRowSecurity == false && sssubquery->cteList == NULL && list_length(sssubquery->rtable) == 1 
										&& sssubquery->jointree != NULL && list_length(sssubquery->jointree->fromlist) == 1 && sssubquery->setOperations == NULL){
										RangeTblEntry *ssrte = (RangeTblEntry *) lfirst(list_head(sssubquery->rtable));
										if (ssrte->rtekind != RTE_RELATION || ssrte->relkind != 'r') {
											allPhyisicalTableFollowRules = false;
											break;
										}
									} else {
										allPhyisicalTableFollowRules = false;
										break;
									}
								} 
							}
							if (allPhyisicalTableFollowRules) {
								*level = 2;
								return true;
							}
						}
					}
				}
			} else { // check if satisfy TwoLevel condition 
				// check if the window func is 3100(row_number)
				TargetEntry *te = NULL;
				foreach_ptr(te, subquery->targetList) {
					if (te->expr != NULL && IsA(te->expr, WindowFunc)) {
						WindowFunc *wf = (WindowFunc *) te->expr;
						if (wf->winfnoid != 3100) {  // window function: row_number()
							if (IsLoggableLevel(DEBUG3)) {
								ereport(DEBUG1, (errmsg("wf->winfnoid != 3100, wf->winfnoid:%d" , wf->winfnoid)));
							}
							return false;
						}
					}
				}
				// subquery don't have from and where statement, only have union
				if (subquery->commandType == CMD_SELECT && subquery->hasTargetSRFs == false && subquery->hasSubLinks == false
					&& subquery->hasDistinctOn == false && subquery->hasRecursive == false && subquery->hasModifyingCTE == false
					&& subquery->hasForUpdate == false && subquery->hasRowSecurity == false && subquery->cteList == NULL 
					&& list_length(subquery->rtable) == 1 && subquery->jointree != NULL && list_length(subquery->jointree->fromlist) == 1) {
					RangeTblEntry *rrte = (RangeTblEntry *) lfirst(list_head(subquery->rtable)); 
					if (rrte->rtekind == RTE_SUBQUERY) {
						// bool secondLevelQueryHasWhere = false;
						// if (subquery->jointree->quals != NULL) {
						// 	secondLevelQueryHasWhere = true;
						// }
						Query* ssubquery = rrte->subquery; // get second level subquery
						// make sure this second level subquery is generate by multi table union on (without join or other complex query)
						if (ssubquery != NULL && ssubquery->commandType == CMD_SELECT && ssubquery->hasAggs == false && ssubquery->hasTargetSRFs == false 
							&& ssubquery->hasSubLinks == false && ssubquery->hasDistinctOn == false && ssubquery->hasRecursive == false
							&& ssubquery->hasModifyingCTE == false && ssubquery->hasForUpdate == false&& ssubquery->hasRowSecurity == false 
							&& ssubquery->cteList == NULL && ssubquery->jointree != NULL && list_length(ssubquery->jointree->fromlist) == 0 
							&& ssubquery->jointree->quals == NULL && ssubquery->setOperations != NULL && ssubquery->hasWindowFuncs == false) {
							bool allPhyisicalTableFollowRules = true;
							ListCell *lc;
							// union on involved tables can not has join or other complex query
							foreach(lc, ssubquery->rtable){ 
								RangeTblEntry *srte = (RangeTblEntry *) lfirst(lc);
								if (srte->rtekind == RTE_SUBQUERY) {
									Query* sssubquery = srte->subquery;
			
									if (sssubquery->commandType == CMD_SELECT && sssubquery->hasAggs == false && sssubquery->hasWindowFuncs == false
										&& sssubquery->hasTargetSRFs == false && sssubquery->hasSubLinks == false && sssubquery->hasDistinctOn == false
										&& sssubquery->hasRecursive == false && sssubquery->hasModifyingCTE == false && sssubquery->hasForUpdate == false
										&& sssubquery->hasRowSecurity == false && sssubquery->cteList == NULL && list_length(sssubquery->rtable) == 1 
										&& sssubquery->jointree != NULL && list_length(sssubquery->jointree->fromlist) == 1 && sssubquery->setOperations == NULL){
										RangeTblEntry *ssrte = (RangeTblEntry *) lfirst(list_head(sssubquery->rtable));
										if (ssrte->rtekind != RTE_RELATION || ssrte->relkind != 'r') {
											allPhyisicalTableFollowRules = false;
											break;
										}
									} else {
										allPhyisicalTableFollowRules = false;
										break;
									}
								} 
							}
							if (allPhyisicalTableFollowRules) {
								*level = 2;
								return true;
							}
						}
					}
				}
			}
		}
	}
	return false;
}

// generate new rte to rteList. Raise the tables that involved by inner view to the outer union all for one level query
static void ExpandOneLevelSimpleViewInvolvedRteList(RangeTblEntry *rte, List **rteList) {
	RangeTblEntry *rteExample = copyObject(rte);
	
	RangeTblEntry *levelOneSrte = (RangeTblEntry *) lfirst(list_head(rte->subquery->rtable)); 
	ListCell   *lc;
	foreach(lc, levelOneSrte->subquery->rtable) {  // each table that view involved
		RangeTblEntry *srte = (RangeTblEntry *) lfirst(lc);
		if (srte->rtekind == RTE_SUBQUERY) {
			RangeTblEntry *newRte = copyObject(rteExample);
			RangeTblEntry *srte2 = (RangeTblEntry *) lfirst(list_head(newRte->subquery->rtable)); 
			srte2->subquery = copyObject(srte->subquery);
			//srte2->subquery = srte->subquery;
			if (IsLoggableLevel(DEBUG3)) {
				//elog_node_display(LOG, "new Query parse tree", newRte->subquery, Debug_pretty_print);
				//ereport(DEBUG3, (errmsg("OneLevelSimpleViewInvolved new Query sql")));
				//StringInfo subqueryString = makeStringInfo();
				//pg_get_query_def(newRte->subquery, subqueryString);
				//ereport(DEBUG3, (errmsg("OneLevelSimpleViewInvolved new Query sql:%s",ApplyLogRedaction(subqueryString->data))));
			}
			*rteList = lappend(*rteList, newRte);	
		}
		//ereport(DEBUG3, (errmsg("1.5  retList:%d, length:%d", *rteList, list_length(*rteList))));
	}
	return;
}

// generate new rte to rteList. Raise the inner view involved tables to the outer union all for two level query
static void ExpandTwoLevelSimpleViewInvolvedRteList(RangeTblEntry *rte, List **rteList) {
	RangeTblEntry *rteExample = copyObject(rte);
	
	RangeTblEntry *levelOneSrte = (RangeTblEntry *) lfirst(list_head(rte->subquery->rtable)); 
	RangeTblEntry *levelTwoSrte = (RangeTblEntry *) lfirst(list_head(levelOneSrte->subquery->rtable)); 
	ListCell   *lc;
	foreach(lc, levelTwoSrte->subquery->rtable) {  // each table that view involved
		RangeTblEntry *srte = (RangeTblEntry *) lfirst(lc);
		if (srte->rtekind == RTE_SUBQUERY) {
			RangeTblEntry *newRte = copyObject(rteExample);
			RangeTblEntry *srte2 = (RangeTblEntry *) lfirst(list_head(newRte->subquery->rtable)); 
			srte2 = (RangeTblEntry *) lfirst(list_head(srte2->subquery->rtable)); 
			// srte2->subquery = copyObject(srte->subquery);
			srte2->subquery = srte->subquery;
			if (IsLoggableLevel(DEBUG3)) {
				//elog_node_display(LOG, "new Query parse tree", newRte->subquery, Debug_pretty_print);
				//ereport(DEBUG3, (errmsg("OneLevelSimpleViewInvolved new Query sql")));
				//StringInfo subqueryString = makeStringInfo();
				//pg_get_query_def(newRte->subquery, subqueryString);
				//ereport(DEBUG3, (errmsg("TwoLevelSimpleViewInvolved new Query sql:%s",ApplyLogRedaction(subqueryString->data))));
			}
			*rteList = lappend(*rteList, newRte);
		}
		//ereport(DEBUG3, (errmsg("1.5  retList:%d, length:%d", *rteList, list_length(*rteList))));
	}
}

// Promote the table involved in the view to the same layer as the outer union all, regenerate rteList
static void ExpandSimpleViewInvolvedTableToUpperLevel(Query *query, Node *node, List **rteList, bool *signal) {
	if (IsA(node, SetOperationStmt))
	{
		SetOperationStmt *setOperations = (SetOperationStmt *) node;
		ExpandSimpleViewInvolvedTableToUpperLevel(query, setOperations->larg, rteList, signal);
		ExpandSimpleViewInvolvedTableToUpperLevel(query, setOperations->rarg, rteList, signal);
	} else if (IsA(node, RangeTblRef)) {
		RangeTblRef *rangeTableRef = (RangeTblRef *) node;
		RangeTblEntry *rangeTableEntry = rt_fetch(rangeTableRef->rtindex,
												  query->rtable);
		if (rangeTableEntry->rtekind != RTE_SUBQUERY) {
			return;
		}
		Query *subquery = rangeTableEntry->subquery;
		int level = 0;
		if (IsOneLevelOrTwoLevelSelectFromSimpleViewQuery(subquery, &level)) {
			*signal = true; 
			if (level == 1) {
				ExpandOneLevelSimpleViewInvolvedRteList(rangeTableEntry, rteList);
			} else if (level == 2) {
				ExpandTwoLevelSimpleViewInvolvedRteList(rangeTableEntry, rteList);
			} else {
				ereport(ERROR, (errmsg("unexpected level (%d) while "
							   "run function IsOneLevelOrTwoLevelSelectFromSimpleViewQuery", level)));
			}
		} else {
			*rteList = lappend(*rteList, rangeTableEntry);
		}
	} else {
		ereport(ERROR, (errmsg("unexpected node type (%d) while "
							   "expecting set operations or "
							   "range table references", nodeTag(node))));
	}
}

/*
 * RecursivelyRegenerateSubqueries finds subqueries, if subquerise match target pattern, regenerate them.
 * current pattern: 
 * 1. if (select XXX(not allow window func) from subquery(some tables linked by union all, 
 *  and each query not have join) where XXX) is a component of Union all, expand the tables involved in the subquery to the upper level
 * 2. if (select  XXX from(select YYY(include window func row_number()) from subquery(some tables linked by union, and each query not have join) where XXX) q1 
 *  where XXX), is a component of Union all, Expand the tables involved in the subquery to the upper level
 * 3. select XXX from subquery(some tables linked by union, and each query not have join) where XXX. For this pattern, where condition can be push down
 * 4. select XXX from (select YYY(include window func row_number()) from subquery(some tables linked by union, and each query not have join) where XXX) q1 where XXX,
 *  both , where condition can be push down
 */
static bool
RecursivelyRegenerateSubqueries(Query *query) {
	//elog_node_display(LOG, "Query parse tree", query, Debug_pretty_print);
	if (query->commandType == CMD_SELECT && query->utilityStmt == NULL && query->hasAggs == false && query->hasWindowFuncs == false 
		&& query->hasTargetSRFs == false && query->hasSubLinks == false && query->hasDistinctOn == false && query->hasRecursive == false 
		&& query->hasModifyingCTE == false && query->hasForUpdate == false && query->hasRowSecurity == false && query->cteList == NULL
		&& query->jointree != NULL && list_length(query->jointree->fromlist) == 0 && query->jointree->quals == NULL && query->setOperations != NULL
		&& IsUnionAllLinkQuery(query, (Node *) query->setOperations)) { //is UNION ALL Query

		// for each subquery which satisfies condition 1 or 2, expand the tables involved in the subquery to the upper level
		List *rteList = NIL; 
		//ereport(DEBUG3, (errmsg("1.0 retList:%d, length:%d", rteList, list_length(rteList))));
		bool signal = false;  // Identify whether the query tree needs to be rebuilt
		ExpandSimpleViewInvolvedTableToUpperLevel(query, (Node *) query->setOperations, &rteList, &signal);
		//ereport(DEBUG3, (errmsg("2.0 retList:%d, length:%d", rteList, list_length(rteList))));
		if (IsLoggableLevel(DEBUG3)) {
			ereport(DEBUG3, (errmsg("############### regenerate rteList length: %d  ################", list_length(rteList))));
		}
		
		if (signal && list_length(rteList) > 0) {
			// according rteList, regenerate query tree
			//ereport(DEBUG3, (errmsg("############### 1.0  ################")));
			ListCell   *lc;
			int i = 0;
			SetOperationStmt *newSetOperStmt = copyObject(query->setOperations);
			//elog_node_display(LOG, "newSetOperStmt  parse tree", newSetOperStmt, Debug_pretty_print);
			newSetOperStmt->larg = NULL;
			newSetOperStmt->rarg = NULL;
			SetOperationStmt *example =  copyObject(newSetOperStmt);
			//elog_node_display(LOG, "example  parse tree", example, Debug_pretty_print);
			SetOperationStmt *head = newSetOperStmt; 
			//ereport(DEBUG3, (errmsg("############### 1.1  ################")));
			foreach(lc, rteList) {
				//ereport(DEBUG3, (errmsg("############### 1.2  ################")));
				RangeTblEntry *rangeTableEntry = (RangeTblEntry *) lfirst(lc);
				// construct SetOperationStmt
				if (i == 0) {
					RangeTblRef *nextRangeTableRef = makeNode(RangeTblRef);
       		     	nextRangeTableRef->rtindex = 1;
					newSetOperStmt->larg = nextRangeTableRef;
				} else if (i==1) {
					RangeTblRef *nextRangeTableRef = makeNode(RangeTblRef);
       		     	nextRangeTableRef->rtindex = 2;
					newSetOperStmt->rarg = nextRangeTableRef;
				} else {
					SetOperationStmt *newNode = copyObject(example);
					newNode->larg = head;
					RangeTblRef *nextRangeTableRef = makeNode(RangeTblRef);
					nextRangeTableRef->rtindex = i+1;
					newNode->rarg = nextRangeTableRef;
					head = newNode;
				}
				//elog_node_display(LOG, "-----head  parse tree", head, Debug_pretty_print);
				i++;
			}
			//ereport(DEBUG3, (errmsg("############### 1.3  ################")));
			//elog_node_display(LOG, "rtable parse tree", rteList, Debug_pretty_print);
			//elog_node_display(LOG, "setOperations parse tree", head, Debug_pretty_print);
			query->rtable = rteList;
			query->setOperations = head;
			query->stmt_location = -1;
			query->stmt_len = -1;
			//elog_node_display(LOG, "Query parse tree", query, Debug_pretty_print);
			if (IsLoggableLevel(DEBUG3)) {
				// StringInfo newString = makeStringInfo();
				// pg_get_query_def(query, newString);
				// ereport(DEBUG3, (errmsg("##### RecursivelyRegenerateSubqueries   ExpandSimpleViewInvolvedTableToUpperLevel  new sql: %s", newString->data)));
			}
			return true;
		}
	} 
	if (query->commandType == CMD_SELECT && query->hasWindowFuncs == false && query->hasTargetSRFs == false && query->hasSubLinks == false
		&& query->hasDistinctOn == false && query->hasRecursive == false && query->hasModifyingCTE == false
		&& query->hasForUpdate == false && query->hasRowSecurity == false && query->cteList == NULL && list_length(query->rtable) == 1
		&& query->jointree != NULL && list_length(query->jointree->fromlist) == 1) {
		// 2. The where statement of the outer sql does not have a statistical function, and only single field is judged
		//bool outerSqlHasWhere = false;
		RangeTblEntry *rte = (RangeTblEntry *) lfirst(list_head(query->rtable));
		if (query->jointree->quals != NULL && rte->rtekind == RTE_SUBQUERY) {
			//outerSqlHasWhere = true;
			Query* subquery = rte->subquery;
			if (subquery->hasWindowFuncs == false) { // deal with condition one
				// subquery don't have from and where statement, only have union
				if (subquery != NULL && subquery->commandType == CMD_SELECT && subquery->hasAggs == false && subquery->hasTargetSRFs == false
					&& subquery->hasSubLinks == false && subquery->hasDistinctOn == false && subquery->hasRecursive == false&& subquery->hasModifyingCTE == false
					&& subquery->hasForUpdate == false&& subquery->hasRowSecurity == false && subquery->cteList == NULL && subquery->jointree != NULL 
					&& list_length(subquery->jointree->fromlist) == 0 && subquery->jointree->quals == NULL && subquery->setOperations != NULL){
					// 3. each union involved sql on query from one table  
					bool allPhyisicalTableFollowRules = true;
					ListCell   *lc;
					foreach(lc, subquery->rtable){
						RangeTblEntry *srte = (RangeTblEntry *) lfirst(lc);
						if (srte->rtekind == RTE_SUBQUERY) {
							Query* ssubquery = srte->subquery;
							// && ssubquery->jointree->quals == NULL && list_length(ssubquery->groupClause) == 0 && list_length(ssubquery->groupingSets) == 0 
							// && list_length(ssubquery->windowClause) == 0 && list_length(ssubquery->distinctClause) == 0 && list_length(ssubquery->sortClause) == 0
							// && ssubquery->limitOffset == NULL && ssubquery->limitCount == NULL
							if (ssubquery->commandType == CMD_SELECT && ssubquery->hasAggs == false && ssubquery->hasWindowFuncs == false&& ssubquery->hasTargetSRFs == false
								&& ssubquery->hasSubLinks == false && ssubquery->hasDistinctOn == false&& ssubquery->hasRecursive == false&& ssubquery->hasModifyingCTE == false
								&& ssubquery->hasForUpdate == false&& ssubquery->hasRowSecurity == false && ssubquery->cteList == NULL && list_length(ssubquery->rtable) == 1 
								&& ssubquery->jointree != NULL && list_length(ssubquery->jointree->fromlist) == 1 && ssubquery->setOperations == NULL){
								RangeTblEntry *ssrte = (RangeTblEntry *) lfirst(list_head(ssubquery->rtable));
								if (ssrte->rtekind != RTE_RELATION || ssrte->relkind != 'r') {
									allPhyisicalTableFollowRules = false;
									break;
								}
							} else {
								allPhyisicalTableFollowRules = false;
								break;
							}
						}
					}
					// 4. then rewrite query, put where condtion to sub-queries and eliminate where condition in outer sql 
					if (allPhyisicalTableFollowRules) {
						// 5. drow where condition from outside sql
						StringInfo subqueryString = makeStringInfo();
						pg_get_query_def(query, subqueryString);
						char* ptr = strstr(subqueryString->data," WHERE ");
						StringInfo whereCondition = makeStringInfo();
						if (ptr != NULL) {
							char* ptr2 = strstr(subqueryString->data," WINDOW ");
							if (ptr2 != NULL) {
								*ptr2 = '\0';
							}
							char* ptr3 = strstr(subqueryString->data," HAVING ");
							if (ptr3 != NULL) {
								*ptr3 = '\0';
							}
							char* ptr4 = strstr(subqueryString->data," GROUP BY ");
							if (ptr4 != NULL) {
								*ptr4 = '\0';
							}
							appendStringInfo(whereCondition,"%s",ptr);
							if (IsLoggableLevel(DEBUG3)) {
								ereport(DEBUG3, (errmsg("################################## whereCondition：%s  ################", whereCondition->data)));
							}
						}
						// 6. drow colume names from subquery
						ListCell   *lc;
						StringInfo columeNames = makeStringInfo();
						foreach(lc, rte->eref->colnames)
						{
							/*
							 * If the column name shown in eref is an empty string, then it's
							 * a column that was dropped at the time of parsing the query, so
							 * treat it as dropped.
							 */
							char *coln = strVal(lfirst(lc));
							if (coln[0] == '\0')
								coln = NULL;
							if (coln != NULL) {
								if (columeNames->len == 0 ) {
									appendStringInfo(columeNames,"%s",coln);
								} else {
									appendStringInfo(columeNames,", %s",coln);
								}
							}	
						}
						if (IsLoggableLevel(DEBUG3)) {
							ereport(DEBUG3, (errmsg("################################## colnames：%s  ################", columeNames->data)));
						}
						// 7. push-down where condition to union on involved table
						foreach(lc, subquery->rtable){
							RangeTblEntry *srte = (RangeTblEntry *) lfirst(lc);
							if (srte->rtekind == RTE_SUBQUERY) {
								Query* ssubquery = srte->subquery;
								StringInfo subqueryFormatString = makeStringInfo();
								pg_get_query_def(ssubquery, subqueryFormatString);
								StringInfo regenerateSqlForWherePushDown = makeStringInfo();
								appendStringInfo(regenerateSqlForWherePushDown,"SELECT %s FROM (%s) test %s", columeNames->data, subqueryFormatString->data, whereCondition->data);
								//Query *newGenerateQuery = ParseQueryString(regenerateSqlForWherePushDown->data, NULL, 0);
								srte->subquery = ParseQueryString(regenerateSqlForWherePushDown->data, NULL, 0);
								// print log
								StringInfo subqueryString2 = makeStringInfo();
								//pg_get_query_def(newGenerateQuery, subqueryString2);
								if (IsLoggableLevel(DEBUG3)) {
									ereport(DEBUG3, (errmsg("$$$$$ before regenerate query:%s" , ApplyLogRedaction(subqueryFormatString->data))));
									ereport(DEBUG1, (errmsg("$$$$$ after regenerate query:%s" , ApplyLogRedaction(regenerateSqlForWherePushDown->data))));
								}
								//ereport(DEBUG1, (errmsg("$$$$$ after regenerate query:%s" , ApplyLogRedaction(subqueryString2->data))));
							}
						}
						subquery->stmt_len = -1;
						subquery->stmt_location = -1;
						// 8. outside sql not need where condition as it has been push down
						query->jointree->quals = NULL;
					}
	
				}
			} else { // deal with conditon two
				// check if the window func is 3100(row_number)
				TargetEntry *te = NULL;
				foreach_ptr(te, subquery->targetList) {
					if (te->expr != NULL && IsA(te->expr, WindowFunc)) {
						WindowFunc *wf = (WindowFunc *) te->expr;
						if (wf->winfnoid != 3100) {  // window function: row_number()
							if (IsLoggableLevel(DEBUG3)) {
								ereport(DEBUG1, (errmsg("wf->winfnoid != 3100, wf->winfnoid:%d" , wf->winfnoid)));
							}
							query_tree_walker(query, RecursivelyRegenerateSubqueryWalker, NULL, 0);
							return true;
						}
					}
				}
				// subquery don't have from and where statement, only have union
				if (subquery->commandType == CMD_SELECT && subquery->hasTargetSRFs == false && subquery->hasSubLinks == false
					&& subquery->hasDistinctOn == false && subquery->hasRecursive == false && subquery->hasModifyingCTE == false
					&& subquery->hasForUpdate == false && subquery->hasRowSecurity == false && subquery->cteList == NULL && list_length(subquery->rtable) == 1
					&& subquery->jointree != NULL && list_length(subquery->jointree->fromlist) == 1) {
					RangeTblEntry *rrte = (RangeTblEntry *) lfirst(list_head(subquery->rtable)); 
					if (rrte->rtekind == RTE_SUBQUERY) {
						bool secondLevelQueryHasWhere = false;
						if (subquery->jointree->quals != NULL) {
							secondLevelQueryHasWhere = true;
						}
						Query* ssubquery = rrte->subquery; // get second level subquery
						// make sure this second level subquery is generate by multi table union on (without join or other complex query)
						if (ssubquery != NULL && ssubquery->commandType == CMD_SELECT && ssubquery->hasAggs == false && ssubquery->hasTargetSRFs == false 
							&& ssubquery->hasSubLinks == false && ssubquery->hasDistinctOn == false && ssubquery->hasRecursive == false&& ssubquery->hasModifyingCTE == false
							&& ssubquery->hasForUpdate == false&& ssubquery->hasRowSecurity == false && ssubquery->cteList == NULL && ssubquery->jointree != NULL 
							&& list_length(ssubquery->jointree->fromlist) == 0 && ssubquery->jointree->quals == NULL && ssubquery->setOperations != NULL 
							&& ssubquery->hasWindowFuncs == false){
							bool allPhyisicalTableFollowRules = true;
							ListCell   *lc;
							// union on involved tables can not has join or other complex query
							foreach(lc, ssubquery->rtable){ 
								RangeTblEntry *srte = (RangeTblEntry *) lfirst(lc);
								if (srte->rtekind == RTE_SUBQUERY) {
									Query* sssubquery = srte->subquery;
			
									if (sssubquery->commandType == CMD_SELECT && sssubquery->hasAggs == false && sssubquery->hasWindowFuncs == false
										&& sssubquery->hasTargetSRFs == false && sssubquery->hasSubLinks == false && sssubquery->hasDistinctOn == false
										&& sssubquery->hasRecursive == false && sssubquery->hasModifyingCTE == false && sssubquery->hasForUpdate == false
										&& sssubquery->hasRowSecurity == false && sssubquery->cteList == NULL && list_length(sssubquery->rtable) == 1 
										&& sssubquery->jointree != NULL && list_length(sssubquery->jointree->fromlist) == 1 && sssubquery->setOperations == NULL){
										RangeTblEntry *ssrte = (RangeTblEntry *) lfirst(list_head(sssubquery->rtable));
										if (ssrte->rtekind != RTE_RELATION || ssrte->relkind != 'r') {
											allPhyisicalTableFollowRules = false;
											break;
										}
									} else {
										allPhyisicalTableFollowRules = false;
										break;
									}
								}
							}
							if (allPhyisicalTableFollowRules) {

								StringInfo targetListString = makeStringInfo();
								pg_get_target_list_def(subquery, targetListString);
			
								StringInfo whereString = makeStringInfo();
								pg_get_where_condition_def(subquery, whereString);
								
								StringInfo outWhereString = makeStringInfo();
								pg_get_where_condition_def(query, outWhereString);
								
								if (IsLoggableLevel(DEBUG3)) {
									ereport(DEBUG3, (errmsg("------subquery targetListString, targetList:%s", targetListString->data)));
									ereport(DEBUG3, (errmsg("------subquery whereString, where condition:%s", whereString->data)));
									ereport(DEBUG3, (errmsg("------subquery outWhereString, where condition:%s", outWhereString->data)));
								}
								foreach(lc, ssubquery->rtable){
									RangeTblEntry *srte = (RangeTblEntry *) lfirst(lc);
									if (srte->rtekind == RTE_SUBQUERY) {
										Query* sssubquery = srte->subquery;
										StringInfo subqueryFormatString = makeStringInfo();
										pg_get_query_def(sssubquery, subqueryFormatString);
										StringInfo regenerateSqlForWherePushDown = makeStringInfo();
										appendStringInfo(regenerateSqlForWherePushDown,"SELECT * FROM (SELECT %s FROM (%s) test WHERE %s) test2 WHERE %s", 
											targetListString->data, subqueryFormatString->data, whereString->data, outWhereString->data);
										//Query *newGenerateQuery = ParseQueryString(regenerateSqlForWherePushDown->data, NULL, 0);
										srte->subquery = ParseQueryString(regenerateSqlForWherePushDown->data, NULL, 0);
										// print log
										//StringInfo subqueryString2 = makeStringInfo();
										//pg_get_query_def(newGenerateQuery, subqueryString2);
										if (IsLoggableLevel(DEBUG3)) {
											ereport(DEBUG3, (errmsg("$$$$$ before regenerate query:%s" , ApplyLogRedaction(subqueryFormatString->data))));
											ereport(DEBUG1, (errmsg("$$$$$ after regenerate query:%s" , ApplyLogRedaction(regenerateSqlForWherePushDown->data))));
										}
									}
								}
								ssubquery->stmt_len = -1;
								ssubquery->stmt_location = -1;
								subquery->jointree->quals = NULL;
							}
						}
					}
				}

			}
		}	
	}
	/* descend into subqueries */
	query_tree_walker(query, RecursivelyRegenerateSubqueryWalker, NULL, 0);
	return true;
}

/*
 * RecursivelyRegenerateSubqueryWalker recursively finds all the Query nodes and
 * recursively Regenerate if necessary.
 */
static bool
RecursivelyRegenerateSubqueryWalker(Node *node)
{
	if (node == NULL)
	{
		return false;
	}

	if (IsA(node, Query))
	{
		
		Query *query = (Query *) node;
		/* ------------- danny test begin ---------------  */
		if (IsLoggableLevel(DEBUG3))
		{
			ereport(DEBUG3, (errmsg("------walk into RecursivelyRegenerateSubqueryWalker and node is query")));
			//StringInfo subqueryString = makeStringInfo();
			//pg_get_query_def(query, subqueryString);
			//ereport(DEBUG3, (errmsg("------walk into RecursivelyRegenerateSubqueryWalker and node is query, query:%s",ApplyLogRedaction(subqueryString->data))));
		}
		/* ------------- danny test  end---------------  */
		/*
		 * First, make sure any subqueries and CTEs within this subquery
		 * are recursively planned if necessary.
		 */
		RecursivelyRegenerateSubqueries(query);

		/* we're done, no need to recurse anymore for this query */
		return false;
	}
	//ereport(DEBUG3, (errmsg("%%%%%%%%%%%%%%%%%%ready to run expression_tree_walker,nodetag:%d,level: :%d ",nodeTag(node),context->level)));
	return expression_tree_walker(node, RecursivelyRegenerateSubqueryWalker, NULL);
}

static bool RecursivelyPlanSetOperationsV2Helper(Query *query, Node *node, int* nodeIds, int* signal, int* nodeIdsLengh, PerNodeUnionSubQueries *perNode) {
	if (IsA(node, SetOperationStmt))
	{
		/* ------------- danny test begin ---------------  */
		if (IsLoggableLevel(DEBUG3))
		{
			ereport(DEBUG3, (errmsg("##### RecursivelyPlanSetOperationsV2Helper is SetOperationStmt")));
		}
		/* ------------- danny test  end---------------  */
		SetOperationStmt *setOperations = (SetOperationStmt *) node;

		bool sign = RecursivelyPlanSetOperationsV2Helper(query, setOperations->larg, nodeIds, signal, nodeIdsLengh, perNode);
		if (sign == false) {
			return sign;
		}
		sign = RecursivelyPlanSetOperationsV2Helper(query, setOperations->rarg, nodeIds, signal, nodeIdsLengh, perNode);
		if (sign == false) {
			return sign;
		}
	} else if (IsA(node, RangeTblRef))
	{
		/* ------------- danny test begin ---------------  */
		if (IsLoggableLevel(DEBUG3))
		{
			ereport(DEBUG3, (errmsg("##### RecursivelyPlanSetOperationsV2Helper is RangeTblRef")));
		}
		/* ------------- danny test  end---------------  */
		RangeTblRef *rangeTableRef = (RangeTblRef *) node;
		RangeTblEntry *rangeTableEntry = rt_fetch(rangeTableRef->rtindex,
												  query->rtable);
		if (rangeTableEntry->rtekind != RTE_SUBQUERY) {
			return false;
		}
		Query *subquery = rangeTableEntry->subquery;
		if (!FindNodeMatchingCheckFunction((Node *) subquery, IsDistributedTableRTE)) {
			return false;
		}
		List *rangeTableList = ExtractRangeTableEntryList(subquery);
		char *nodeName = NULL;
		uint32 nodePort = 0;
		uint32 nodeId  = 0;
		ListCell *rangeTableCell = NULL;
		bool findFirstRTE = false;
		foreach(rangeTableCell, rangeTableList) {
			RangeTblEntry *rte = (RangeTblEntry *) lfirst(rangeTableCell);
			if (IsLoggableLevel(DEBUG3)) {
				ereport(DEBUG3, (errmsg("angeTableEntry->rtekind:%d, rangeTableEntry->relid:%d", rte->rtekind, rte->relid)));
			}
			Oid distributedTableId = rte->relid;
			if (rte->rtekind == RTE_RELATION && rte->relkind == 'r') {
				Oid distributedTableId = rte->relid;
				if (findFirstRTE == true) {
					return false;
				}
				findFirstRTE = true;
				CitusTableCacheEntry *cacheEntry = GetCitusTableCacheEntry(distributedTableId);
				if (cacheEntry->shardIntervalArrayLength != 1) {
					return false;
				}
				ShardInterval *newShardInterval = CopyShardInterval(cacheEntry->sortedShardIntervalArray[0]);
				if (IsLoggableLevel(DEBUG3)) {
					ereport(DEBUG3, (errmsg("distributedTableId:%d, shardId:%d", distributedTableId, newShardInterval->shardId)));
				}
				List *shardPlacementList = ActiveShardPlacementList(newShardInterval->shardId);
				ShardPlacement *shardPlacement = NULL;
				foreach_ptr(shardPlacement, shardPlacementList)
				{
					if (IsLoggableLevel(DEBUG3)) {
						ereport(DEBUG3, (errmsg("nodeName:%s, nodePort:%d, nodeId:%d", shardPlacement->nodeName, shardPlacement->nodePort, shardPlacement->nodeId)));
					}
					if (shardPlacement->shardState == SHARD_STATE_ACTIVE)
					{
						if (signal[shardPlacement->nodeId] == 0) {
							// PerNodeUnionSubQueries *node = (PerNodeUnionSubQueries*) palloc0(sizeof(PerNodeUnionSubQueries));
							// node->nodeName = shardPlacement->nodeName;
							// node->nodePort = shardPlacement->nodePort;
							// node->nodeId = shardPlacement->nodeId;
							// node->rtindex = rangeTableRef->rtindex;

							PerNodeUnionSubQueries node;
							node.subquerise = NIL;
							node.nodeName = shardPlacement->nodeName;
							node.nodePort = shardPlacement->nodePort;
							node.nodeId = shardPlacement->nodeId;
							node.rtindex = rangeTableRef->rtindex;
							// todo
							//node.subquerise = lappend(node->subquerise, subquery);
							node.subquerise = lappend(node.subquerise, subquery);
							//perNode[node->nodeId] = *node;
							perNode[node.nodeId] = node;
							//nodeIds[*nodeIdsLengh] = node->nodeId;
							nodeIds[*nodeIdsLengh] = node.nodeId;
							*nodeIdsLengh = *nodeIdsLengh + 1;
							signal[shardPlacement->nodeId] = 1; // mark this nodeId exist
						} else {
							perNode[shardPlacement->nodeId].subquerise = lappend(perNode[shardPlacement->nodeId].subquerise, subquery);
						}
					} else {
						return false;
					}
					break;
				}
			}
		}
	} else
	{
		ereport(ERROR, (errmsg("unexpected node type (%d) while "
							   "expecting set operations or "
							   "range table references", nodeTag(node))));
	}
	return true;
}

/*
 * RecursivelyPlanSetOperationsV2 determines if regenerate Query Tree
 * if union all involved tables locate in same worker, then gether subquery together
 */
static bool 
RecursivelyPlanSetOperationsV2(Query *query, Node *node) {
	PerNodeUnionSubQueries *perNode = palloc0(1000 * sizeof(PerNodeUnionSubQueries));
	memset(perNode, 0, 1000 * sizeof(PerNodeUnionSubQueries));
	int *nodeIds = palloc0(1000 * sizeof(int));
	int *signal = palloc0(1000 * sizeof(int));
	memset(nodeIds, 0, 1000 * sizeof(int));
	memset(signal, 0, 1000 * sizeof(int));
	int nodeIdsLengh = 0;
	// Group union according to the node where the table is located
	bool sign = RecursivelyPlanSetOperationsV2Helper(query, node, nodeIds, signal, &nodeIdsLengh, perNode);
	if (!sign) {
		return false;
	}
	if (IsLoggableLevel(DEBUG3)) {
		ereport(DEBUG3, (errmsg("##### RecursivelyPlanSetOperationsV2  nodeIdsLengh:%d", nodeIdsLengh)));
		for (int i =0 ;i < nodeIdsLengh; i++) {
			ereport(DEBUG3, (errmsg("##### RecursivelyPlanSetOperationsV2 i:%d,  nodeIds:%d, nodeName:%s, nodePort:%d, subquerise-size:%d", 
				i, nodeIds[i], perNode[nodeIds[i]].nodeName, perNode[nodeIds[i]].nodePort, list_length(perNode[nodeIds[i]].subquerise))));
		}
	}
	if (nodeIdsLengh <= 1) {
		return false;
	}
	// regenerate SetOperationStmt, Combine the unions that can be executed on the same worker
	List *rteList = NIL; 

	SetOperationStmt *newSetOperStmt = copyObject(query->setOperations);
	newSetOperStmt->larg = NULL;
	newSetOperStmt->rarg = NULL;
	SetOperationStmt *example =  copyObject(newSetOperStmt);
	SetOperationStmt *head = newSetOperStmt;

	for (int i =0 ;i < nodeIdsLengh; i++) {
		// construct RangeTblEntry
		RangeTblEntry *rangeTableEntry = rt_fetch(perNode[nodeIds[i]].rtindex, query->rtable);
		if (list_length(perNode[nodeIds[i]].subquerise) == 1) {
			rteList = lappend(rteList, rangeTableEntry);
		} else {
			StringInfo regenerateSqlForCombineUnion = makeStringInfo();  
		
			Query *subquery = NULL;
			bool first = true;
			foreach_ptr(subquery, perNode[nodeIds[i]].subquerise) {
				StringInfo subqueryFormatString = makeStringInfo();
				pg_get_query_def(subquery, subqueryFormatString);
				if (first) {
					appendStringInfo(regenerateSqlForCombineUnion,"%s", subqueryFormatString->data);
					first = false;
				} else {
					appendStringInfo(regenerateSqlForCombineUnion," UNION ALL %s", subqueryFormatString->data);
				}
			}
			if (list_length(perNode[nodeIds[i]].subquerise) > 1) {
				StringInfo regenerateSqlForFinal = makeStringInfo();
				appendStringInfo(regenerateSqlForFinal,"SELECT * FROM (%s) set", regenerateSqlForCombineUnion->data);
				regenerateSqlForCombineUnion = regenerateSqlForFinal;
			}
			if (IsLoggableLevel(DEBUG3)) {
				ereport(DEBUG3, (errmsg("##### RecursivelyPlanSetOperationsV2  regenerateSqlForCombineUnion :%s", regenerateSqlForCombineUnion->data)));
			}

			rangeTableEntry->subquery = ParseQueryString(regenerateSqlForCombineUnion->data, NULL, 0);
			rteList = lappend(rteList, rangeTableEntry);
		}
		// construct SetOperationStmt
		if (i == 0) {
			RangeTblRef *nextRangeTableRef = makeNode(RangeTblRef);
            nextRangeTableRef->rtindex = 1;
			newSetOperStmt->larg = nextRangeTableRef;
		} else if (i==1) {
			RangeTblRef *nextRangeTableRef = makeNode(RangeTblRef);
            nextRangeTableRef->rtindex = 2;
			newSetOperStmt->rarg = nextRangeTableRef;
		} else {
			SetOperationStmt *newNode = copyObject(example);
			newNode->larg = head;
			RangeTblRef *nextRangeTableRef = makeNode(RangeTblRef);
			nextRangeTableRef->rtindex = i+1;
			newNode->rarg = nextRangeTableRef;
			head = newNode;
		}
	}

	query->rtable = rteList;
	query->setOperations = head;
	query->stmt_location = -1;
	query->stmt_len = -1;
	if (IsLoggableLevel(DEBUG3)) {
		StringInfo newString = makeStringInfo();
		pg_get_query_def(query, newString);
		ereport(DEBUG3, (errmsg("##### RecursivelyPlanSetOperationsV2  newString :%s", newString->data)));
	}
	for (int i =0 ;i < nodeIdsLengh; i++) {
		list_free(perNode[nodeIds[i]].subquerise);
	}
	pfree(perNode);
	pfree(nodeIds);
	pfree(signal);
	return true;
}


static bool
RecursivelyRegenerateUnionAll(Query *query) {
	if (query->setOperations != NULL) {
		bool signal = RecursivelyPlanSetOperationsV2(query, (Node *) query->setOperations);
		if (signal) {
			return false;
		}
	} 
	query_tree_walker(query, RecursivelyRegenerateUnionAllWalker, NULL, 0);
	return true;
}

static bool
RecursivelyRegenerateUnionAllWalker(Node *node){
	if (node == NULL)
	{
		return false;
	}

	if (IsA(node, Query))
	{
		
		Query *query = (Query *) node;
		 
		RecursivelyRegenerateUnionAll(query);

		/* we're done, no need to recurse anymore for this query */
		return false;
	}
	//ereport(DEBUG3, (errmsg("%%%%%%%%%%%%%%%%%%ready to run expression_tree_walker,nodetag:%d,level: :%d ",nodeTag(node),context->level)));
	return expression_tree_walker(node, RecursivelyRegenerateUnionAllWalker, NULL);
}


/* ------------- danny test end ---------------  */
/* Distributed planner hook */
PlannedStmt *
distributed_planner(Query *parse,
	#if PG_VERSION_NUM >= PG_VERSION_13
					const char *query_string,
	#endif
					int cursorOptions,
					ParamListInfo boundParams)
{
	bool needsDistributedPlanning = false;
	bool fastPathRouterQuery = false;
	Node *distributionKeyValue = NULL;

	List *rangeTableList = ExtractRangeTableEntryList(parse);

	/* ------------- danny test begin ---------------  */
	if (IsLoggableLevel(DEBUG1))
	{
		ereport(DEBUG1, (errmsg("------walk into distributed_planner-------")));
		print_mem(getpid(), "distributed_planner");
		// StringInfo subqueryString = makeStringInfo();
		// pg_get_query_def(parse, subqueryString);
		// ereport(DEBUG1, (errmsg("------walk into distributed_planner, query:%s",ApplyLogRedaction(subqueryString->data))));
		
		// ListCell *rangeTableCell = NULL;

		// foreach(rangeTableCell, rangeTableList)
		// {
		// 	RangeTblEntry *rangeTableEntry = (RangeTblEntry *) lfirst(rangeTableCell);
		// 	ereport(DEBUG3, (errmsg("angeTableEntry->rtekind:%d, rangeTableEntry->relid:%d", rangeTableEntry->rtekind, rangeTableEntry->relid)));
		// }
	}
	/* ------------- danny test  end---------------  */

	if (cursorOptions & CURSOR_OPT_FORCE_DISTRIBUTED)
	{
		/* this cursor flag could only be set when Citus has been loaded */
		Assert(CitusHasBeenLoaded());

		needsDistributedPlanning = true;
	}
	else if (CitusHasBeenLoaded())
	{
		needsDistributedPlanning = ListContainsDistributedTableRTE(rangeTableList);
		if (needsDistributedPlanning)
		{
			ereport(DEBUG1, (errmsg("Pid:%d, NextPlanId:%d", Pid, NextPlanId)));
			/* ------------- danny test begin ---------------  */
			// if (Pid != NextPlanId) {
			// 	Pid = NextPlanId;
				TimestampTz startTimestamp = GetCurrentTimestamp();
				long durationSeconds = 0.0;
				int durationMicrosecs = 0;
				long durationMillisecs = 0.0;
				print_mem(getpid(), "before RecursivelyRegenerateSubqueries");
				MemoryContext functionCallContext = AllocSetContextCreate(CurrentMemoryContext,
															  "Regenerate Subqueries Function Call Context",
															  ALLOCSET_DEFAULT_MINSIZE,
															  ALLOCSET_DEFAULT_INITSIZE,
															  ALLOCSET_DEFAULT_MAXSIZE);
				MemoryContext oldContext = MemoryContextSwitchTo(functionCallContext);

				RecursivelyRegenerateSubqueries(parse);
				
				print_mem(getpid(), "after RecursivelyRegenerateSubqueries");
				// StringInfo subqueryString = makeStringInfo();
				// pg_get_query_def(parse, subqueryString);
				// print_mem(getpid(), "after RecursivelyRegenerateSubqueries and pg_get_query_def");
				/*
				 * We have copied whatever we needed from the UDF calls, so we can free
				 * the memory allocated by them.
				 */
				//MemoryContextSwitchTo(oldContext);
				// as we modify parse tree structure, some record like location is not right, thus need to regenerate this tree
				//parse = ParseQueryString(subqueryString->data, NULL, 0);
				//MemoryContextReset(functionCallContext);
				print_mem(getpid(), "after RecursivelyRegenerateSubqueries and ParseQueryString");
				TimestampDifference(startTimestamp, GetCurrentTimestamp(), &durationSeconds,
								&durationMicrosecs);
				durationMillisecs = durationSeconds * SECOND_TO_MILLI_SECOND;
				durationMillisecs += durationMicrosecs * MICRO_TO_MILLI_SECOND;
				if (IsLoggableLevel(DEBUG1)) {
					//ereport(DEBUG1, (errmsg("~~~~~~~~~~~init regenerate query:%s" , ApplyLogRedaction(subqueryString->data))));
					ereport(DEBUG1, (errmsg("-------run RecursivelyRegenerateSubqueries, do union on flatten and where condition push down,start_time:%s, reparse query tree time cost:%d",
					 timestamptz_to_str(startTimestamp), durationMillisecs)));
				}
				// MemoryContext functionCallContext2 = AllocSetContextCreate(CurrentMemoryContext,
				// 											  "Recursively Regenerate UnionAllWalker Function Call Context",
				// 											  ALLOCSET_DEFAULT_MINSIZE,
				// 											  ALLOCSET_DEFAULT_INITSIZE,
				// 											  ALLOCSET_DEFAULT_MAXSIZE);
				//MemoryContext oldContext2 = MemoryContextSwitchTo(functionCallContext2);
				print_mem(getpid(), "after RecursivelyRegenerateUnionAllWalker");
				RecursivelyRegenerateUnionAllWalker(parse);
				print_mem(getpid(), "after RecursivelyRegenerateUnionAllWalker");
				StringInfo subqueryString = makeStringInfo();
				pg_get_query_def(parse, subqueryString);
				//ereport(DEBUG1, (errmsg("~~~~~~~~~~~init regenerate query:%s" , ApplyLogRedaction(subqueryString->data))));
				print_mem(getpid(), "after RecursivelyRegenerateUnionAllWalker and pg_get_query_def");
				MemoryContextSwitchTo(oldContext);
				
				parse = ParseQueryString(subqueryString->data, NULL, 0);
				MemoryContextReset(functionCallContext);
				//MemoryContextReset(functionCallContext2);
				rangeTableList = ExtractRangeTableEntryList(parse);
				//RegenerateSubqueries = true;
			//}
			//ereport(DEBUG3, (errmsg("~~~~~~~~~~~ finish ExtractRangeTableEntryList")));
			/* ------------- danny test end ---------------  */
			fastPathRouterQuery = FastPathRouterQuery(parse, &distributionKeyValue);
		}
	}
	//ereport(DEBUG3, (errmsg("~~~~~~~~~~~ 1.0 ")));
	int rteIdCounter = 1;
	print_mem(getpid(), "before construct planContext");
	DistributedPlanningContext planContext = {
		.query = parse,
		.cursorOptions = cursorOptions,
		.boundParams = boundParams,
	};

	if (fastPathRouterQuery)
	{
		/*
		 *  We need to copy the parse tree because the FastPathPlanner modifies
		 *  it. In the next branch we do the same for other distributed queries
		 *  too, but for those it needs to be done AFTER calling
		 *  AssignRTEIdentities.
		 */
		planContext.originalQuery = copyObject(parse);
		print_mem(getpid(), "if (fastPathRouterQuery)");
	}
	else if (needsDistributedPlanning)
	{

		/*
		 * standard_planner scribbles on it's input, but for deparsing we need the
		 * unmodified form. Note that before copying we call
		 * AssignRTEIdentities, which is needed because these identities need
		 * to be present in the copied query too.
		 */
		rteIdCounter = AssignRTEIdentities(rangeTableList, rteIdCounter);
		planContext.originalQuery = copyObject(parse);
		print_mem(getpid(), "1.1");
		bool setPartitionedTablesInherited = false;
		AdjustPartitioningForDistributedPlanning(rangeTableList,
												 setPartitionedTablesInherited);
		print_mem(getpid(), "1.2");
	}

	/*
	 * Make sure that we hide shard names on the Citus MX worker nodes. See comments in
	 * ReplaceTableVisibleFunction() for the details.
	 */
	print_mem(getpid(), "1.3");
	ReplaceTableVisibleFunction((Node *) parse);
	print_mem(getpid(), "1.4");
	/* create a restriction context and put it at the end if context list */
	planContext.plannerRestrictionContext = CreateAndPushPlannerRestrictionContext();
	print_mem(getpid(), "1.5");
	/*
	 * We keep track of how many times we've recursed into the planner, primarily
	 * to detect whether we are in a function call. We need to make sure that the
	 * PlannerLevel is decremented exactly once at the end of the next PG_TRY
	 * block, both in the happy case and when an error occurs.
	 */
	PlannerLevel++;

	PlannedStmt *result = NULL;

	PG_TRY();
	{
		//ereport(DEBUG3, (errmsg("~~~~~~~~~~~ 1.1 ")));
		if (fastPathRouterQuery)
		{
			result = PlanFastPathDistributedStmt(&planContext, distributionKeyValue);
			print_mem(getpid(), "1.6");
		}
		else
		{
			//ereport(DEBUG3, (errmsg("~~~~~~~~~~~ 1.2 ")));
			/*
			 * Call into standard_planner because the Citus planner relies on both the
			 * restriction information per table and parse tree transformations made by
			 * postgres' planner.
			 */
			planContext.plan = standard_planner_compat(planContext.query,
													   planContext.cursorOptions,
													   planContext.boundParams);
			print_mem(getpid(), "1.7");
			if (needsDistributedPlanning)
			{
				//ereport(DEBUG3, (errmsg("~~~~~~~~~~~ 1.3 ")));
				result = PlanDistributedStmt(&planContext, rteIdCounter);
				print_mem(getpid(), "1.8");
			}
			else if ((result = TryToDelegateFunctionCall(&planContext)) == NULL)
			{
				result = planContext.plan;
				print_mem(getpid(), "1.9");
			}
			//ereport(DEBUG3, (errmsg("~~~~~~~~~~~ 1.4 ")));
		}
	}
	PG_CATCH();
	{
		PopPlannerRestrictionContext();

		PlannerLevel--;

		PG_RE_THROW();
	}
	PG_END_TRY();

	PlannerLevel--;

	/* remove the context from the context list */
	PopPlannerRestrictionContext();
	print_mem(getpid(), "2.0");
	/*
	 * In some cases, for example; parameterized SQL functions, we may miss that
	 * there is a need for distributed planning. Such cases only become clear after
	 * standard_planner performs some modifications on parse tree. In such cases
	 * we will simply error out.
	 */
	if (!needsDistributedPlanning && NeedsDistributedPlanning(parse))
	{
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg("cannot perform distributed planning on this "
							   "query because parameterized queries for SQL "
							   "functions referencing distributed tables are "
							   "not supported"),
						errhint("Consider using PL/pgSQL functions instead.")));
	}

	return result;
}


/*
 * ExtractRangeTableEntryList is a wrapper around ExtractRangeTableEntryWalker.
 * The function traverses the input query and returns all the range table
 * entries that are in the query tree.
 */
List *
ExtractRangeTableEntryList(Query *query)
{
	List *rteList = NIL;

	ExtractRangeTableEntryWalker((Node *) query, &rteList);

	return rteList;
}


/*
 * NeedsDistributedPlanning returns true if the Citus extension is loaded and
 * the query contains a distributed table.
 *
 * This function allows queries containing local tables to pass through the
 * distributed planner. How to handle local tables is a decision that should
 * be made within the planner
 */
bool
NeedsDistributedPlanning(Query *query)
{
	if (!CitusHasBeenLoaded())
	{
		return false;
	}

	CmdType commandType = query->commandType;

	if (commandType != CMD_SELECT && commandType != CMD_INSERT &&
		commandType != CMD_UPDATE && commandType != CMD_DELETE)
	{
		return false;
	}

	List *allRTEs = ExtractRangeTableEntryList(query);

	return ListContainsDistributedTableRTE(allRTEs);
}


/*
 * ListContainsDistributedTableRTE gets a list of range table entries
 * and returns true if there is at least one distributed relation range
 * table entry in the list.
 */
static bool
ListContainsDistributedTableRTE(List *rangeTableList)
{
	ListCell *rangeTableCell = NULL;

	foreach(rangeTableCell, rangeTableList)
	{
		RangeTblEntry *rangeTableEntry = (RangeTblEntry *) lfirst(rangeTableCell);

		if (rangeTableEntry->rtekind != RTE_RELATION)
		{
			continue;
		}

		if (IsCitusTable(rangeTableEntry->relid))
		{
			return true;
		}
	}

	return false;
}


/*
 * AssignRTEIdentities function modifies query tree by adding RTE identities to the
 * RTE_RELATIONs.
 *
 * Please note that, we want to avoid modifying query tree as much as possible
 * because if PostgreSQL changes the way it uses modified fields, that may break
 * our logic.
 *
 * Returns the next id. This can be used to call on a rangeTableList that may've
 * been partially assigned. Should be set to 1 initially.
 */
static int
AssignRTEIdentities(List *rangeTableList, int rteIdCounter)
{
	ListCell *rangeTableCell = NULL;

	foreach(rangeTableCell, rangeTableList)
	{
		RangeTblEntry *rangeTableEntry = (RangeTblEntry *) lfirst(rangeTableCell);

		/*
		 * To be able to track individual RTEs through PostgreSQL's query
		 * planning, we need to be able to figure out whether an RTE is
		 * actually a copy of another, rather than a different one. We
		 * simply number the RTEs starting from 1.
		 *
		 * Note that we're only interested in RTE_RELATIONs and thus assigning
		 * identifiers to those RTEs only.
		 */
		if (rangeTableEntry->rtekind == RTE_RELATION &&
			rangeTableEntry->values_lists == NIL)
		{
			AssignRTEIdentity(rangeTableEntry, rteIdCounter++);
		}
	}

	return rteIdCounter;
}


/*
 * AdjustPartitioningForDistributedPlanning function modifies query tree by
 * changing inh flag and relkind of partitioned tables. We want Postgres to
 * treat partitioned tables as regular relations (i.e. we do not want to
 * expand them to their partitions) since it breaks Citus planning in different
 * ways. We let anything related to partitioning happen on the shards.
 *
 * Please note that, we want to avoid modifying query tree as much as possible
 * because if PostgreSQL changes the way it uses modified fields, that may break
 * our logic.
 */
static void
AdjustPartitioningForDistributedPlanning(List *rangeTableList,
										 bool setPartitionedTablesInherited)
{
	ListCell *rangeTableCell = NULL;

	foreach(rangeTableCell, rangeTableList)
	{
		RangeTblEntry *rangeTableEntry = (RangeTblEntry *) lfirst(rangeTableCell);

		/*
		 * We want Postgres to behave partitioned tables as regular relations
		 * (i.e. we do not want to expand them to their partitions). To do this
		 * we set each partitioned table's inh flag to appropriate
		 * value before and after dropping to the standart_planner.
		 */
		if (rangeTableEntry->rtekind == RTE_RELATION &&
			PartitionedTable(rangeTableEntry->relid))
		{
			rangeTableEntry->inh = setPartitionedTablesInherited;

			if (setPartitionedTablesInherited)
			{
				rangeTableEntry->relkind = RELKIND_PARTITIONED_TABLE;
			}
			else
			{
				rangeTableEntry->relkind = RELKIND_RELATION;
			}
		}
	}
}


/*
 * AssignRTEIdentity assigns the given rteIdentifier to the given range table
 * entry.
 *
 * To be able to track RTEs through postgres' query planning, which copies and
 * duplicate, and modifies them, we sometimes need to figure out whether two
 * RTEs are copies of the same original RTE. For that we, hackishly, use a
 * field normally unused in RTE_RELATION RTEs.
 *
 * The assigned identifier better be unique within a plantree.
 */
static void
AssignRTEIdentity(RangeTblEntry *rangeTableEntry, int rteIdentifier)
{
	Assert(rangeTableEntry->rtekind == RTE_RELATION);

	rangeTableEntry->values_lists = list_make1_int(rteIdentifier);
}


/* GetRTEIdentity returns the identity assigned with AssignRTEIdentity. */
int
GetRTEIdentity(RangeTblEntry *rte)
{
	Assert(rte->rtekind == RTE_RELATION);
	Assert(rte->values_lists != NIL);
	Assert(IsA(rte->values_lists, IntList));
	Assert(list_length(rte->values_lists) == 1);

	return linitial_int(rte->values_lists);
}


/*
 * GetQueryLockMode returns the necessary lock mode to be acquired for the
 * given query. (See comment written in RangeTblEntry->rellockmode)
 */
LOCKMODE
GetQueryLockMode(Query *query)
{
	if (IsModifyCommand(query))
	{
		return RowExclusiveLock;
	}
	else if (query->hasForUpdate)
	{
		return RowShareLock;
	}
	else
	{
		return AccessShareLock;
	}
}


/*
 * IsModifyCommand returns true if the query performs modifications, false
 * otherwise.
 */
bool
IsModifyCommand(Query *query)
{
	CmdType commandType = query->commandType;

	if (commandType == CMD_INSERT || commandType == CMD_UPDATE ||
		commandType == CMD_DELETE)
	{
		return true;
	}

	return false;
}


/*
 * IsMultiTaskPlan returns true if job contains multiple tasks.
 */
bool
IsMultiTaskPlan(DistributedPlan *distributedPlan)
{
	Job *workerJob = distributedPlan->workerJob;

	if (workerJob != NULL && list_length(workerJob->taskList) > 1)
	{
		return true;
	}

	return false;
}


/*
 * IsUpdateOrDelete returns true if the query performs an update or delete.
 */
bool
IsUpdateOrDelete(Query *query)
{
	return query->commandType == CMD_UPDATE ||
		   query->commandType == CMD_DELETE;
}


/*
 * PlanFastPathDistributedStmt creates a distributed planned statement using
 * the FastPathPlanner.
 */
static PlannedStmt *
PlanFastPathDistributedStmt(DistributedPlanningContext *planContext,
							Node *distributionKeyValue)
{
	FastPathRestrictionContext *fastPathContext =
		planContext->plannerRestrictionContext->fastPathRestrictionContext;

	planContext->plannerRestrictionContext->fastPathRestrictionContext->
	fastPathRouterQuery = true;

	if (distributionKeyValue == NULL)
	{
		/* nothing to record */
	}
	else if (IsA(distributionKeyValue, Const))
	{
		fastPathContext->distributionKeyValue = (Const *) distributionKeyValue;
	}
	else if (IsA(distributionKeyValue, Param))
	{
		fastPathContext->distributionKeyHasParam = true;
	}

	planContext->plan = FastPathPlanner(planContext->originalQuery, planContext->query,
										planContext->boundParams);

	return CreateDistributedPlannedStmt(planContext);
}


/*
 * PlanDistributedStmt creates a distributed planned statement using the PG
 * planner.
 */
static PlannedStmt *
PlanDistributedStmt(DistributedPlanningContext *planContext,
					int rteIdCounter)
{
	/* may've inlined new relation rtes */
	List *rangeTableList = ExtractRangeTableEntryList(planContext->query);
	rteIdCounter = AssignRTEIdentities(rangeTableList, rteIdCounter);


	PlannedStmt *result = CreateDistributedPlannedStmt(planContext);

	bool setPartitionedTablesInherited = true;
	AdjustPartitioningForDistributedPlanning(rangeTableList,
											 setPartitionedTablesInherited);

	return result;
}


/*
 * DissuadePlannerFromUsingPlan try dissuade planner when planning a plan that
 * potentially failed due to unresolved prepared statement parameters.
 */
void
DissuadePlannerFromUsingPlan(PlannedStmt *plan)
{
	/*
	 * Arbitrarily high cost, but low enough that it can be added up
	 * without overflowing by choose_custom_plan().
	 */
	plan->planTree->total_cost = FLT_MAX / 100000000;
}


/*
 * CreateDistributedPlannedStmt encapsulates the logic needed to transform a particular
 * query into a distributed plan that is encapsulated by a PlannedStmt.
 */
static PlannedStmt *
CreateDistributedPlannedStmt(DistributedPlanningContext *planContext)
{
	uint64 planId = NextPlanId++;
	bool hasUnresolvedParams = false;

	PlannedStmt *resultPlan = NULL;

	if (QueryTreeContainsInlinableCTE(planContext->originalQuery))
	{
		/*
		 * Inlining CTEs as subqueries in the query can avoid recursively
		 * planning some (or all) of the CTEs. In other words, the inlined
		 * CTEs could become part of query pushdown planning, which is much
		 * more efficient than recursively planning. So, first try distributed
		 * planning on the inlined CTEs in the query tree.
		 *
		 * We also should fallback to distributed planning with non-inlined CTEs
		 * if the distributed planning fails with inlined CTEs, because recursively
		 * planning CTEs can provide full SQL coverage, although it might be slow.
		 */
		resultPlan = InlineCtesAndCreateDistributedPlannedStmt(planId, planContext);
		if (resultPlan != NULL)
		{
			return resultPlan;
		}
	}

	if (HasUnresolvedExternParamsWalker((Node *) planContext->originalQuery,
										planContext->boundParams))
	{
		hasUnresolvedParams = true;
	}
	/* ------------- danny test begin ---------------  */
	if (IsLoggableLevel(DEBUG3))
	{
		if (hasUnresolvedParams) {
			ereport(DEBUG3, (errmsg("---hasUnresolvedParams is true---")));
		} else {
			ereport(DEBUG3, (errmsg("---hasUnresolvedParams is false---")));
		}
	}
	/* ------------- danny test  end---------------  */
	DistributedPlan *distributedPlan =
		CreateDistributedPlan(planId, planContext->originalQuery, planContext->query,
							  planContext->boundParams,
							  hasUnresolvedParams,
							  planContext->plannerRestrictionContext);

	/*
	 * If no plan was generated, prepare a generic error to be emitted.
	 * Normally this error message will never returned to the user, as it's
	 * usually due to unresolved prepared statement parameters - in that case
	 * the logic below will force a custom plan (i.e. with parameters bound to
	 * specific values) to be generated.  But sql (not plpgsql) functions
	 * unfortunately don't go through a codepath supporting custom plans - so
	 * we still need to have an error prepared.
	 */
	if (!distributedPlan)
	{
		/* currently always should have a more specific error otherwise */
		Assert(hasUnresolvedParams);
		distributedPlan = CitusMakeNode(DistributedPlan);
		distributedPlan->planningError =
			DeferredError(ERRCODE_FEATURE_NOT_SUPPORTED,
						  "could not create distributed plan",
						  "Possibly this is caused by the use of parameters in SQL "
						  "functions, which is not supported in Citus.",
						  "Consider using PL/pgSQL functions instead.");
	}

	/*
	 * Error out if none of the planners resulted in a usable plan, unless the
	 * error was possibly triggered by missing parameters.  In that case we'll
	 * not error out here, but instead rely on postgres' custom plan logic.
	 * Postgres re-plans prepared statements the first five executions
	 * (i.e. it produces custom plans), after that the cost of a generic plan
	 * is compared with the average custom plan cost.  We support otherwise
	 * unsupported prepared statement parameters by assigning an exorbitant
	 * cost to the unsupported query.  That'll lead to the custom plan being
	 * chosen.  But for that to be possible we can't error out here, as
	 * otherwise that logic is never reached.
	 */
	if (distributedPlan->planningError && !hasUnresolvedParams)
	{
		RaiseDeferredError(distributedPlan->planningError, ERROR);
	}

	/* remember the plan's identifier for identifying subplans */
	distributedPlan->planId = planId;

	/* create final plan by combining local plan with distributed plan */
	resultPlan = FinalizePlan(planContext->plan, distributedPlan);

	/*
	 * As explained above, force planning costs to be unrealistically high if
	 * query planning failed (possibly) due to prepared statement parameters or
	 * if it is planned as a multi shard modify query.
	 */
	if ((distributedPlan->planningError ||
		 (IsUpdateOrDelete(planContext->originalQuery) && IsMultiTaskPlan(
			  distributedPlan))) &&
		hasUnresolvedParams)
	{
		DissuadePlannerFromUsingPlan(resultPlan);
	}

	return resultPlan;
}


/*
 * InlineCtesAndCreateDistributedPlannedStmt gets all the parameters required
 * for creating a distributed planned statement. The function is primarily a
 * wrapper on top of CreateDistributedPlannedStmt(), by first inlining the
 * CTEs and calling CreateDistributedPlannedStmt() in PG_TRY() block. The
 * function returns NULL if the planning fails on the query where eligable
 * CTEs are inlined.
 */
static PlannedStmt *
InlineCtesAndCreateDistributedPlannedStmt(uint64 planId,
										  DistributedPlanningContext *planContext)
{
	if (!EnableCTEInlining)
	{
		/*
		 * In Postgres 12+, users can adjust whether to inline/not inline CTEs
		 * by [NOT] MATERIALIZED keywords. However, in PG 11, that's not possible.
		 * So, with this we provide a way to prevent CTE inlining on Postgres 11.
		 *
		 * The main use-case for this is not to have divergent test outputs between
		 * PG 11 vs PG 12, so not very much intended for users.
		 */
		return NULL;
	}

	/*
	 * We'll inline the CTEs and try distributed planning, preserve the original
	 * query in case the planning fails and we fallback to recursive planning of
	 * CTEs.
	 */
	Query *copyOfOriginalQuery = copyObject(planContext->originalQuery);

	RecursivelyInlineCtesInQueryTree(copyOfOriginalQuery);

	/* after inlining, we shouldn't have any inlinable CTEs */
	Assert(!QueryTreeContainsInlinableCTE(copyOfOriginalQuery));

#if PG_VERSION_NUM < PG_VERSION_12
	Query *query = planContext->query;

	/*
	 * We had to implement this hack because on Postgres11 and below, the originalQuery
	 * and the query would have significant differences in terms of CTEs where CTEs
	 * would not be inlined on the query (as standard_planner() wouldn't inline CTEs
	 * on PG 11 and below).
	 *
	 * Instead, we prefer to pass the inlined query to the distributed planning. We rely
	 * on the fact that the query includes subqueries, and it'd definitely go through
	 * query pushdown planning. During query pushdown planning, the only relevant query
	 * tree is the original query.
	 */
	planContext->query = copyObject(copyOfOriginalQuery);
#endif


	/* simply recurse into CreateDistributedPlannedStmt() in a PG_TRY() block */
	PlannedStmt *result = TryCreateDistributedPlannedStmt(planContext->plan,
														  copyOfOriginalQuery,
														  planContext->query,
														  planContext->boundParams,
														  planContext->
														  plannerRestrictionContext);

#if PG_VERSION_NUM < PG_VERSION_12

	/*
	 * Set back the original query, in case the planning failed and we need to go
	 * into distributed planning again.
	 */
	planContext->query = query;
#endif

	return result;
}


/*
 * TryCreateDistributedPlannedStmt is a wrapper around CreateDistributedPlannedStmt, simply
 * calling it in PG_TRY()/PG_CATCH() block. The function returns a PlannedStmt if the input
 * query can be planned by Citus. If not, the function returns NULL and generates a DEBUG4
 * message with the reason for the failure.
 */
static PlannedStmt *
TryCreateDistributedPlannedStmt(PlannedStmt *localPlan,
								Query *originalQuery,
								Query *query, ParamListInfo boundParams,
								PlannerRestrictionContext *plannerRestrictionContext)
{
	MemoryContext savedContext = CurrentMemoryContext;
	PlannedStmt *result = NULL;

	DistributedPlanningContext *planContext = palloc0(sizeof(DistributedPlanningContext));

	planContext->plan = localPlan;
	planContext->boundParams = boundParams;
	planContext->originalQuery = originalQuery;
	planContext->query = query;
	planContext->plannerRestrictionContext = plannerRestrictionContext;


	PG_TRY();
	{
		result = CreateDistributedPlannedStmt(planContext);
	}
	PG_CATCH();
	{
		MemoryContextSwitchTo(savedContext);
		ErrorData *edata = CopyErrorData();
		FlushErrorState();

		/* don't try to intercept PANIC or FATAL, let those breeze past us */
		if (edata->elevel != ERROR)
		{
			PG_RE_THROW();
		}

		ereport(DEBUG4, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						 errmsg("Planning after CTEs inlined failed with "
								"\nmessage: %s\ndetail: %s\nhint: %s",
								edata->message ? edata->message : "",
								edata->detail ? edata->detail : "",
								edata->hint ? edata->hint : "")));

		/* leave the error handling system */
		FreeErrorData(edata);

		result = NULL;
	}
	PG_END_TRY();

	return result;
}


/*
 * CreateDistributedPlan generates a distributed plan for a query.
 * It goes through 3 steps:
 *
 * 1. Try router planner
 * 2. Generate subplans for CTEs and complex subqueries
 *    - If any, go back to step 1 by calling itself recursively
 * 3. Logical planner
 */
DistributedPlan *
CreateDistributedPlan(uint64 planId, Query *originalQuery, Query *query, ParamListInfo
					  boundParams, bool hasUnresolvedParams,
					  PlannerRestrictionContext *plannerRestrictionContext)
{
	DistributedPlan *distributedPlan = NULL;
	bool hasCtes = originalQuery->cteList != NIL;

	if (IsModifyCommand(originalQuery))
	{
		EnsureModificationsCanRun();

		Oid targetRelationId = ModifyQueryResultRelationId(query);
		EnsurePartitionTableNotReplicated(targetRelationId);

		if (InsertSelectIntoCitusTable(originalQuery))
		{
			if (hasUnresolvedParams)
			{
				/*
				 * Unresolved parameters can cause performance regressions in
				 * INSERT...SELECT when the partition column is a parameter
				 * because we don't perform any additional pruning in the executor.
				 */
				return NULL;
			}

			distributedPlan =
				CreateInsertSelectPlan(planId, originalQuery, plannerRestrictionContext,
									   boundParams);
		}
		else if (InsertSelectIntoLocalTable(originalQuery))
		{
			if (hasUnresolvedParams)
			{
				/*
				 * Unresolved parameters can cause performance regressions in
				 * INSERT...SELECT when the partition column is a parameter
				 * because we don't perform any additional pruning in the executor.
				 */
				return NULL;
			}
			distributedPlan =
				CreateInsertSelectIntoLocalTablePlan(planId, originalQuery, boundParams,
													 hasUnresolvedParams,
													 plannerRestrictionContext);
		}
		else
		{
			/* modifications are always routed through the same planner/executor */
			distributedPlan =
				CreateModifyPlan(originalQuery, query, plannerRestrictionContext);
		}

		/* the functions above always return a plan, possibly with an error */
		Assert(distributedPlan);

		if (distributedPlan->planningError == NULL)
		{
			return distributedPlan;
		}
		else
		{
			RaiseDeferredError(distributedPlan->planningError, DEBUG2);
		}
	}
	else
	{
		/*
		 * For select queries we, if router executor is enabled, first try to
		 * plan the query as a router query. If not supported, otherwise try
		 * the full blown plan/optimize/physical planning process needed to
		 * produce distributed query plans.
		 */
		/* ------------- danny test begin ---------------  */
		if (IsLoggableLevel(DEBUG3))
		{
			ereport(DEBUG3, (errmsg("prepare to CreateRouterPlan")));
		}
		/* ------------- danny test  end---------------  */
		distributedPlan = CreateRouterPlan(originalQuery, query,
										   plannerRestrictionContext);
		
		if (distributedPlan->planningError == NULL)
		{
			/* ------------- danny test begin ---------------  */
			if (IsLoggableLevel(DEBUG3))
			{
				ereport(DEBUG3, (errmsg("CreateRouterPlan success")));
			}
			/* ------------- danny test  end---------------  */
			return distributedPlan;
		}
		else
		{
			/* ------------- danny test begin ---------------  */
			if (IsLoggableLevel(DEBUG3))
			{
				ereport(DEBUG3, (errmsg("CreateRouterPlan fail")));
			}
			/* ------------- danny test  end---------------  */
			/*
			 * For debugging it's useful to display why query was not
			 * router plannable.
			 */
			RaiseDeferredError(distributedPlan->planningError, DEBUG2);
		}
	}

	if (hasUnresolvedParams)
	{
		/*
		 * There are parameters that don't have a value in boundParams.
		 *
		 * The remainder of the planning logic cannot handle unbound
		 * parameters. We return a NULL plan, which will have an
		 * extremely high cost, such that postgres will replan with
		 * bound parameters.
		 */
		return NULL;
	}

	/* force evaluation of bound params */
	boundParams = copyParamList(boundParams);

	/*
	 * If there are parameters that do have a value in boundParams, replace
	 * them in the original query. This allows us to more easily cut the
	 * query into pieces (during recursive planning) or deparse parts of
	 * the query (during subquery pushdown planning).
	 */
	originalQuery = (Query *) ResolveExternalParams((Node *) originalQuery,
													boundParams);
	Assert(originalQuery != NULL);
	/* ------------- danny test begin ---------------  */
	if (IsLoggableLevel(DEBUG3))
	{
		ereport(DEBUG3, (errmsg("ready to run GenerateSubplansForSubqueriesAndCTEs, planId:%d", planId)));
	}
	if (IsLoggableLevel(DEBUG3))
	{
		StringInfo subqueryString = makeStringInfo();

		pg_get_query_def(originalQuery, subqueryString);

		ereport(DEBUG3, (errmsg("planId:%d, query:%s" ,planId, ApplyLogRedaction(subqueryString->data))));
	}
	/* ------------- danny test  end---------------  */
	/*
	 * Plan subqueries and CTEs that cannot be pushed down by recursively
	 * calling the planner and return the resulting plans to subPlanList.
	 */
	List *subPlanList = GenerateSubplansForSubqueriesAndCTEs(planId, originalQuery,
															 plannerRestrictionContext);
	/* ------------- danny test begin ---------------  */
	if (IsLoggableLevel(DEBUG3))
	{
		ereport(DEBUG3, (errmsg("after GenerateSubplansForSubqueriesAndCTEs, subPlanList size is %d", list_length(subPlanList))));
	}
	/* ------------- danny test  end---------------  */
	/*
	 * If subqueries were recursively planned then we need to replan the query
	 * to get the new planner restriction context and apply planner transformations.
	 *
	 * We could simplify this code if the logical planner was capable of dealing
	 * with an original query. In that case, we would only have to filter the
	 * planner restriction context.
	 *
	 * Note that we check both for subplans and whether the query had CTEs
	 * prior to calling GenerateSubplansForSubqueriesAndCTEs. If none of
	 * the CTEs are referenced then there are no subplans, but we still want
	 * to retry the router planner.
	 */
	if (list_length(subPlanList) > 0 || hasCtes)
	{
		Query *newQuery = copyObject(originalQuery);
		bool setPartitionedTablesInherited = false;
		PlannerRestrictionContext *currentPlannerRestrictionContext =
			CurrentPlannerRestrictionContext();

		/* reset the current planner restrictions context */
		ResetPlannerRestrictionContext(currentPlannerRestrictionContext);

		/*
		 * We force standard_planner to treat partitioned tables as regular tables
		 * by clearing the inh flag on RTEs. We already did this at the start of
		 * distributed_planner, but on a copy of the original query, so we need
		 * to do it again here.
		 */
		AdjustPartitioningForDistributedPlanning(ExtractRangeTableEntryList(newQuery),
												 setPartitionedTablesInherited);

		/*
		 * Some relations may have been removed from the query, but we can skip
		 * AssignRTEIdentities since we currently do not rely on RTE identities
		 * being contiguous.
		 */

		standard_planner_compat(newQuery, 0, boundParams);

		/* overwrite the old transformed query with the new transformed query */
		*query = *newQuery;

		/* recurse into CreateDistributedPlan with subqueries/CTEs replaced */
		/* ------------- danny test begin ---------------  */
		if (IsLoggableLevel(DEBUG3))
		{
			ereport(DEBUG3, (errmsg("recurse into CreateDistributedPlan")));
		}
		/* ------------- danny test  end---------------  */
		distributedPlan = CreateDistributedPlan(planId, originalQuery, query, NULL, false,
												plannerRestrictionContext);

		/* distributedPlan cannot be null since hasUnresolvedParams argument was false */
		Assert(distributedPlan != NULL);
		distributedPlan->subPlanList = subPlanList;

		return distributedPlan;
	}
	/* ------------- danny test begin ---------------  */
	if (IsLoggableLevel(DEBUG3))
	{
		ereport(DEBUG3, (errmsg("ready to check IsModifyCommand")));
	}
	/* ------------- danny test  end---------------  */
	/*
	 * DML command returns a planning error, even after recursive planning. The
	 * logical planner cannot handle DML commands so return the plan with the
	 * error.
	 */
	/* ------------- danny test begin ---------------  */
	if (IsModifyCommand(originalQuery))
	{
		return distributedPlan;
	}
	/* ------------- danny test  end---------------  */

	/*
	 * CTEs are stripped from the original query by RecursivelyPlanSubqueriesAndCTEs.
	 * If we get here and there are still CTEs that means that none of the CTEs are
	 * referenced. We therefore also strip the CTEs from the rewritten query.
	 */
	query->cteList = NIL;
	Assert(originalQuery->cteList == NIL);
	/* ------------- danny test begin ---------------  */
	if (IsLoggableLevel(DEBUG3))
	{
		ereport(DEBUG3, (errmsg("ready to run MultiLogicalPlanCreate")));
	}
	/* ------------- danny test  end---------------  */
	MultiTreeRoot *logicalPlan = MultiLogicalPlanCreate(originalQuery, query,
														plannerRestrictionContext);
	MultiLogicalPlanOptimize(logicalPlan);

	/*
	 * This check is here to make it likely that all node types used in
	 * Citus are dumpable. Explain can dump logical and physical plans
	 * using the extended outfuncs infrastructure, but it's infeasible to
	 * test most plans. MultiQueryContainerNode always serializes the
	 * physical plan, so there's no need to check that separately
	 */
	CheckNodeIsDumpable((Node *) logicalPlan);
	/* ------------- danny test begin ---------------  */
	if (IsLoggableLevel(DEBUG3))
	{
		ereport(DEBUG3, (errmsg("ready to run CreatePhysicalDistributedPlan")));
	}
	/* ------------- danny test  end---------------  */
	/* Create the physical plan */
	distributedPlan = CreatePhysicalDistributedPlan(logicalPlan,
													plannerRestrictionContext);

	/* distributed plan currently should always succeed or error out */
	Assert(distributedPlan && distributedPlan->planningError == NULL);

	return distributedPlan;
}


/*
 * EnsurePartitionTableNotReplicated errors out if the infput relation is
 * a partition table and the table has a replication factor greater than
 * one.
 *
 * If the table is not a partition or replication factor is 1, the function
 * becomes a no-op.
 */
void
EnsurePartitionTableNotReplicated(Oid relationId)
{
	DeferredErrorMessage *deferredError =
		DeferErrorIfPartitionTableNotSingleReplicated(relationId);
	if (deferredError != NULL)
	{
		RaiseDeferredError(deferredError, ERROR);
	}
}


/*
 * DeferErrorIfPartitionTableNotSingleReplicated defers error if the input relation
 * is a partition table with replication factor > 1. Otherwise, the function returns
 * NULL.
 */
static DeferredErrorMessage *
DeferErrorIfPartitionTableNotSingleReplicated(Oid relationId)
{
	if (PartitionTableNoLock(relationId) && !SingleReplicatedTable(relationId))
	{
		Oid parentOid = PartitionParentOid(relationId);
		char *parentRelationTest = get_rel_name(parentOid);
		StringInfo errorHint = makeStringInfo();

		appendStringInfo(errorHint, "Run the query on the parent table "
									"\"%s\" instead.", parentRelationTest);

		return DeferredError(ERRCODE_FEATURE_NOT_SUPPORTED,
							 "modifications on partitions when replication "
							 "factor is greater than 1 is not supported",
							 NULL, errorHint->data);
	}

	return NULL;
}


/*
 * ResolveExternalParams replaces the external parameters that appears
 * in the query with the corresponding entries in the boundParams.
 *
 * Note that this function is inspired by eval_const_expr() on Postgres.
 * We cannot use that function because it requires access to PlannerInfo.
 */
Node *
ResolveExternalParams(Node *inputNode, ParamListInfo boundParams)
{
	/* consider resolving external parameters only when boundParams exists */
	if (!boundParams)
	{
		return inputNode;
	}

	if (inputNode == NULL)
	{
		return NULL;
	}

	if (IsA(inputNode, Param))
	{
		Param *paramToProcess = (Param *) inputNode;
		int numberOfParameters = boundParams->numParams;
		int parameterId = paramToProcess->paramid;
		int16 typeLength = 0;
		bool typeByValue = false;
		Datum constValue = 0;

		if (paramToProcess->paramkind != PARAM_EXTERN)
		{
			return inputNode;
		}

		if (parameterId < 0)
		{
			return inputNode;
		}

		/* parameterId starts from 1 */
		int parameterIndex = parameterId - 1;
		if (parameterIndex >= numberOfParameters)
		{
			return inputNode;
		}

		ParamExternData *correspondingParameterData =
			&boundParams->params[parameterIndex];

		if (!(correspondingParameterData->pflags & PARAM_FLAG_CONST))
		{
			return inputNode;
		}

		get_typlenbyval(paramToProcess->paramtype, &typeLength, &typeByValue);

		bool paramIsNull = correspondingParameterData->isnull;
		if (paramIsNull)
		{
			constValue = 0;
		}
		else if (typeByValue)
		{
			constValue = correspondingParameterData->value;
		}
		else
		{
			/*
			 * Out of paranoia ensure that datum lives long enough,
			 * although bind params currently should always live
			 * long enough.
			 */
			constValue = datumCopy(correspondingParameterData->value, typeByValue,
								   typeLength);
		}

		return (Node *) makeConst(paramToProcess->paramtype, paramToProcess->paramtypmod,
								  paramToProcess->paramcollid, typeLength, constValue,
								  paramIsNull, typeByValue);
	}
	else if (IsA(inputNode, Query))
	{
		return (Node *) query_tree_mutator((Query *) inputNode, ResolveExternalParams,
										   boundParams, 0);
	}

	return expression_tree_mutator(inputNode, ResolveExternalParams, boundParams);
}


/*
 * GetDistributedPlan returns the associated DistributedPlan for a CustomScan.
 *
 * Callers should only read from the returned data structure, since it may be
 * the plan of a prepared statement and may therefore be reused.
 */
DistributedPlan *
GetDistributedPlan(CustomScan *customScan)
{
	Assert(list_length(customScan->custom_private) == 1);

	Node *node = (Node *) linitial(customScan->custom_private);
	Assert(CitusIsA(node, DistributedPlan));

	CheckNodeCopyAndSerialization(node);

	DistributedPlan *distributedPlan = (DistributedPlan *) node;

	return distributedPlan;
}


/*
 * FinalizePlan combines local plan with distributed plan and creates a plan
 * which can be run by the PostgreSQL executor.
 */
PlannedStmt *
FinalizePlan(PlannedStmt *localPlan, DistributedPlan *distributedPlan)
{
	PlannedStmt *finalPlan = NULL;
	CustomScan *customScan = makeNode(CustomScan);
	MultiExecutorType executorType = MULTI_EXECUTOR_INVALID_FIRST;

	/* this field is used in JobExecutorType */
	distributedPlan->relationIdList = localPlan->relationOids;

	if (!distributedPlan->planningError)
	{
		executorType = JobExecutorType(distributedPlan);
	}

	switch (executorType)
	{
		case MULTI_EXECUTOR_ADAPTIVE:
		{
			customScan->methods = &AdaptiveExecutorCustomScanMethods;
			break;
		}

		case MULTI_EXECUTOR_NON_PUSHABLE_INSERT_SELECT:
		{
			customScan->methods = &NonPushableInsertSelectCustomScanMethods;
			break;
		}

		default:
		{
			customScan->methods = &DelayedErrorCustomScanMethods;
			break;
		}
	}

	if (IsMultiTaskPlan(distributedPlan))
	{
		/* if it is not a single task executable plan, inform user according to the log level */
		if (MultiTaskQueryLogLevel != CITUS_LOG_LEVEL_OFF)
		{
			ereport(MultiTaskQueryLogLevel, (errmsg(
												 "multi-task query about to be executed"),
											 errhint(
												 "Queries are split to multiple tasks "
												 "if they have to be split into several"
												 " queries on the workers.")));
		}
	}

	distributedPlan->queryId = localPlan->queryId;

	Node *distributedPlanData = (Node *) distributedPlan;

	customScan->custom_private = list_make1(distributedPlanData);
	customScan->flags = CUSTOMPATH_SUPPORT_BACKWARD_SCAN;

	/*
	 * Fast path queries cannot have any subplans by definition, so skip
	 * expensive traversals.
	 */
	if (!distributedPlan->fastPathRouterPlan)
	{
		/*
		 * Record subplans used by distributed plan to make intermediate result
		 * pruning easier.
		 *
		 * We do this before finalizing the plan, because the combineQuery is
		 * rewritten by standard_planner in FinalizeNonRouterPlan.
		 */
		distributedPlan->usedSubPlanNodeList = FindSubPlanUsages(distributedPlan);
	}

	if (distributedPlan->combineQuery)
	{
		finalPlan = FinalizeNonRouterPlan(localPlan, distributedPlan, customScan);
	}
	else
	{
		finalPlan = FinalizeRouterPlan(localPlan, customScan);
	}

	return finalPlan;
}


/*
 * FinalizeNonRouterPlan gets the distributed custom scan plan, and creates the
 * final master select plan on the top of this distributed plan for adaptive executor.
 */
static PlannedStmt *
FinalizeNonRouterPlan(PlannedStmt *localPlan, DistributedPlan *distributedPlan,
					  CustomScan *customScan)
{
	PlannedStmt *finalPlan = PlanCombineQuery(distributedPlan, customScan);
	finalPlan->queryId = localPlan->queryId;
	finalPlan->utilityStmt = localPlan->utilityStmt;

	/* add original range table list for access permission checks */
	finalPlan->rtable = list_concat(finalPlan->rtable, localPlan->rtable);

	return finalPlan;
}


/*
 * FinalizeRouterPlan gets a CustomScan node which already wrapped distributed
 * part of a router plan and sets it as the direct child of the router plan
 * because we don't run any query on master node for router executable queries.
 * Here, we also rebuild the column list to read from the remote scan.
 */
static PlannedStmt *
FinalizeRouterPlan(PlannedStmt *localPlan, CustomScan *customScan)
{
	List *columnNameList = NIL;

	customScan->custom_scan_tlist =
		makeCustomScanTargetlistFromExistingTargetList(localPlan->planTree->targetlist);
	customScan->scan.plan.targetlist =
		makeTargetListFromCustomScanList(customScan->custom_scan_tlist);

	/* extract the column names from the final targetlist*/
	TargetEntry *targetEntry = NULL;
	foreach_ptr(targetEntry, customScan->scan.plan.targetlist)
	{
		Value *columnName = makeString(targetEntry->resname);
		columnNameList = lappend(columnNameList, columnName);
	}

	PlannedStmt *routerPlan = makeNode(PlannedStmt);
	routerPlan->planTree = (Plan *) customScan;

	RangeTblEntry *remoteScanRangeTableEntry = RemoteScanRangeTableEntry(columnNameList);
	routerPlan->rtable = list_make1(remoteScanRangeTableEntry);

	/* add original range table list for access permission checks */
	routerPlan->rtable = list_concat(routerPlan->rtable, localPlan->rtable);

	routerPlan->canSetTag = true;
	routerPlan->relationOids = NIL;

	routerPlan->queryId = localPlan->queryId;
	routerPlan->utilityStmt = localPlan->utilityStmt;
	routerPlan->commandType = localPlan->commandType;
	routerPlan->hasReturning = localPlan->hasReturning;

	return routerPlan;
}


/*
 * makeCustomScanTargetlistFromExistingTargetList rebuilds the targetlist from the remote
 * query into a list that can be used as the custom_scan_tlist for our Citus Custom Scan.
 */
static List *
makeCustomScanTargetlistFromExistingTargetList(List *existingTargetlist)
{
	List *custom_scan_tlist = NIL;

	/* we will have custom scan range table entry as the first one in the list */
	const int customScanRangeTableIndex = 1;

	/* build a targetlist to read from the custom scan output */
	TargetEntry *targetEntry = NULL;
	foreach_ptr(targetEntry, existingTargetlist)
	{
		Assert(IsA(targetEntry, TargetEntry));

		/*
		 * This is unlikely to be hit because we would not need resjunk stuff
		 * at the toplevel of a router query - all things needing it have been
		 * pushed down.
		 */
		if (targetEntry->resjunk)
		{
			continue;
		}

		/* build target entry pointing to remote scan range table entry */
		Var *newVar = makeVarFromTargetEntry(customScanRangeTableIndex, targetEntry);

		if (newVar->vartype == RECORDOID || newVar->vartype == RECORDARRAYOID)
		{
			/*
			 * Add the anonymous composite type to the type cache and store
			 * the key in vartypmod. Eventually this makes its way into the
			 * TupleDesc used by the executor, which uses it to parse the
			 * query results from the workers in BuildTupleFromCStrings.
			 */
			newVar->vartypmod = BlessRecordExpression(targetEntry->expr);
		}

		TargetEntry *newTargetEntry = flatCopyTargetEntry(targetEntry);
		newTargetEntry->expr = (Expr *) newVar;
		custom_scan_tlist = lappend(custom_scan_tlist, newTargetEntry);
	}

	return custom_scan_tlist;
}


/*
 * makeTargetListFromCustomScanList based on a custom_scan_tlist create the target list to
 * use on the Citus Custom Scan Node. The targetlist differs from the custom_scan_tlist in
 * a way that the expressions in the targetlist all are references to the index (resno) in
 * the custom_scan_tlist in their varattno while the varno is replaced with INDEX_VAR
 * instead of the range table entry index.
 */
static List *
makeTargetListFromCustomScanList(List *custom_scan_tlist)
{
	List *targetList = NIL;
	TargetEntry *targetEntry = NULL;
	int resno = 1;
	foreach_ptr(targetEntry, custom_scan_tlist)
	{
		/*
		 * INDEX_VAR is used to reference back to the TargetEntry in custom_scan_tlist by
		 * its resno (index)
		 */
		Var *newVar = makeVarFromTargetEntry(INDEX_VAR, targetEntry);
		TargetEntry *newTargetEntry = makeTargetEntry((Expr *) newVar, resno,
													  targetEntry->resname,
													  targetEntry->resjunk);
		targetList = lappend(targetList, newTargetEntry);
		resno++;
	}
	return targetList;
}


/*
 * BlessRecordExpression ensures we can parse an anonymous composite type on the
 * target list of a query that is sent to the worker.
 *
 * We cannot normally parse record types coming from the workers unless we
 * "bless" the tuple descriptor, which adds a transient type to the type cache
 * and assigns it a type mod value, which is the key in the type cache.
 */
int32
BlessRecordExpression(Expr *expr)
{
	int32 typeMod = -1;

	if (IsA(expr, FuncExpr) || IsA(expr, OpExpr))
	{
		/*
		 * Handle functions that return records on the target
		 * list, e.g. SELECT function_call(1,2);
		 */
		Oid resultTypeId = InvalidOid;
		TupleDesc resultTupleDesc = NULL;

		/* get_expr_result_type blesses the tuple descriptor */
		TypeFuncClass typeClass = get_expr_result_type((Node *) expr, &resultTypeId,
													   &resultTupleDesc);

		if (typeClass == TYPEFUNC_COMPOSITE)
		{
			typeMod = resultTupleDesc->tdtypmod;
		}
	}
	else if (IsA(expr, RowExpr))
	{
		/*
		 * Handle row expressions, e.g. SELECT (1,2);
		 */
		RowExpr *rowExpr = (RowExpr *) expr;
		TupleDesc rowTupleDesc = NULL;
		ListCell *argCell = NULL;
		int currentResno = 1;

#if PG_VERSION_NUM >= PG_VERSION_12
		rowTupleDesc = CreateTemplateTupleDesc(list_length(rowExpr->args));
#else
		rowTupleDesc = CreateTemplateTupleDesc(list_length(rowExpr->args), false);
#endif

		foreach(argCell, rowExpr->args)
		{
			Node *rowArg = (Node *) lfirst(argCell);
			Oid rowArgTypeId = exprType(rowArg);
			int rowArgTypeMod = exprTypmod(rowArg);

			if (rowArgTypeId == RECORDOID || rowArgTypeId == RECORDARRAYOID)
			{
				/* ensure nested rows are blessed as well */
				rowArgTypeMod = BlessRecordExpression((Expr *) rowArg);
			}

			TupleDescInitEntry(rowTupleDesc, currentResno, NULL,
							   rowArgTypeId, rowArgTypeMod, 0);
			TupleDescInitEntryCollation(rowTupleDesc, currentResno,
										exprCollation(rowArg));

			currentResno++;
		}

		BlessTupleDesc(rowTupleDesc);

		typeMod = rowTupleDesc->tdtypmod;
	}
	else if (IsA(expr, ArrayExpr))
	{
		/*
		 * Handle row array expressions, e.g. SELECT ARRAY[(1,2)];
		 * Postgres allows ARRAY[(1,2),(1,2,3)]. We do not.
		 */
		ArrayExpr *arrayExpr = (ArrayExpr *) expr;

		typeMod = BlessRecordExpressionList(arrayExpr->elements);
	}
	else if (IsA(expr, NullIfExpr))
	{
		NullIfExpr *nullIfExpr = (NullIfExpr *) expr;

		typeMod = BlessRecordExpressionList(nullIfExpr->args);
	}
	else if (IsA(expr, MinMaxExpr))
	{
		MinMaxExpr *minMaxExpr = (MinMaxExpr *) expr;

		typeMod = BlessRecordExpressionList(minMaxExpr->args);
	}
	else if (IsA(expr, CoalesceExpr))
	{
		CoalesceExpr *coalesceExpr = (CoalesceExpr *) expr;

		typeMod = BlessRecordExpressionList(coalesceExpr->args);
	}
	else if (IsA(expr, CaseExpr))
	{
		CaseExpr *caseExpr = (CaseExpr *) expr;
		List *results = NIL;
		ListCell *whenCell = NULL;

		foreach(whenCell, caseExpr->args)
		{
			CaseWhen *whenArg = (CaseWhen *) lfirst(whenCell);

			results = lappend(results, whenArg->result);
		}

		if (caseExpr->defresult != NULL)
		{
			results = lappend(results, caseExpr->defresult);
		}

		typeMod = BlessRecordExpressionList(results);
	}

	return typeMod;
}


/*
 * BlessRecordExpressionList maps BlessRecordExpression over a list.
 * Returns typmod of all expressions, or -1 if they are not all the same.
 * Ignores expressions with a typmod of -1.
 */
static int32
BlessRecordExpressionList(List *exprs)
{
	int32 finalTypeMod = -1;
	ListCell *exprCell = NULL;
	foreach(exprCell, exprs)
	{
		Node *exprArg = (Node *) lfirst(exprCell);
		int32 exprTypeMod = BlessRecordExpression((Expr *) exprArg);

		if (exprTypeMod == -1)
		{
			continue;
		}
		else if (finalTypeMod == -1)
		{
			finalTypeMod = exprTypeMod;
		}
		else if (finalTypeMod != exprTypeMod)
		{
			return -1;
		}
	}
	return finalTypeMod;
}


/*
 * RemoteScanRangeTableEntry creates a range table entry from given column name
 * list to represent a remote scan.
 */
RangeTblEntry *
RemoteScanRangeTableEntry(List *columnNameList)
{
	RangeTblEntry *remoteScanRangeTableEntry = makeNode(RangeTblEntry);

	/* we use RTE_VALUES for custom scan because we can't look up relation */
	remoteScanRangeTableEntry->rtekind = RTE_VALUES;
	remoteScanRangeTableEntry->eref = makeAlias("remote_scan", columnNameList);
	remoteScanRangeTableEntry->inh = false;
	remoteScanRangeTableEntry->inFromCl = true;

	return remoteScanRangeTableEntry;
}


/*
 * CheckNodeIsDumpable checks that the passed node can be dumped using
 * nodeToString(). As this checks is expensive, it's only active when
 * assertions are enabled.
 */
static void
CheckNodeIsDumpable(Node *node)
{
#ifdef USE_ASSERT_CHECKING
	char *out = nodeToString(node);
	pfree(out);
#endif
}


/*
 * CheckNodeCopyAndSerialization checks copy/dump/read functions
 * for nodes and returns copy of the input.
 *
 * It is only active when assertions are enabled, otherwise it returns
 * the input directly. We use this to confirm that our serialization
 * and copy logic produces the correct plan during regression tests.
 *
 * It does not check string equality on node dumps due to differences
 * in some Postgres types.
 */
static Node *
CheckNodeCopyAndSerialization(Node *node)
{
#ifdef USE_ASSERT_CHECKING
	char *out = nodeToString(node);
	Node *nodeCopy = copyObject(node);
	char *outCopy = nodeToString(nodeCopy);

	pfree(out);
	pfree(outCopy);

	return nodeCopy;
#else
	return node;
#endif
}


/*
 * multi_join_restriction_hook is a hook called by postgresql standard planner
 * to notify us about various planning information regarding joins. We use
 * it to learn about the joining column.
 */
void
multi_join_restriction_hook(PlannerInfo *root,
							RelOptInfo *joinrel,
							RelOptInfo *outerrel,
							RelOptInfo *innerrel,
							JoinType jointype,
							JoinPathExtraData *extra)
{
	if (bms_is_empty(innerrel->relids) || bms_is_empty(outerrel->relids))
	{
		/*
		 * We do not expect empty relids. Still, ignoring such JoinRestriction is
		 * preferable for two reasons:
		 * 1. This might be a query that doesn't rely on JoinRestrictions at all (e.g.,
		 * local query).
		 * 2. We cannot process them when they are empty (and likely to segfault if
		 * we allow as-is).
		 */
		ereport(DEBUG1, (errmsg("Join restriction information is NULL")));
	}

	/*
	 * Use a memory context that's guaranteed to live long enough, could be
	 * called in a more shortly lived one (e.g. with GEQO).
	 */
	PlannerRestrictionContext *plannerRestrictionContext =
		CurrentPlannerRestrictionContext();
	MemoryContext restrictionsMemoryContext = plannerRestrictionContext->memoryContext;
	MemoryContext oldMemoryContext = MemoryContextSwitchTo(restrictionsMemoryContext);

	JoinRestrictionContext *joinRestrictionContext =
		plannerRestrictionContext->joinRestrictionContext;
	Assert(joinRestrictionContext != NULL);

	JoinRestriction *joinRestriction = palloc0(sizeof(JoinRestriction));
	joinRestriction->joinType = jointype;
	joinRestriction->plannerInfo = root;

	/*
	 * We create a copy of restrictInfoList and relids because with geqo they may
	 * be created in a memory context which will be deleted when we still need it,
	 * thus we create a copy of it in our memory context.
	 */
	joinRestriction->joinRestrictInfoList = copyObject(extra->restrictlist);
	joinRestriction->innerrelRelids = bms_copy(innerrel->relids);
	joinRestriction->outerrelRelids = bms_copy(outerrel->relids);

	joinRestrictionContext->joinRestrictionList =
		lappend(joinRestrictionContext->joinRestrictionList, joinRestriction);

	/*
	 * Keep track if we received any semi joins here. If we didn't we can
	 * later safely convert any semi joins in the rewritten query to inner
	 * joins.
	 */
	joinRestrictionContext->hasSemiJoin = joinRestrictionContext->hasSemiJoin ||
										  extra->sjinfo->jointype == JOIN_SEMI;

	MemoryContextSwitchTo(oldMemoryContext);
}


/*
 * multi_relation_restriction_hook is a hook called by postgresql standard planner
 * to notify us about various planning information regarding a relation. We use
 * it to retrieve restrictions on relations.
 */
void
multi_relation_restriction_hook(PlannerInfo *root, RelOptInfo *relOptInfo,
								Index restrictionIndex, RangeTblEntry *rte)
{
	CitusTableCacheEntry *cacheEntry = NULL;

	if (ReplaceCitusExtraDataContainer && IsCitusExtraDataContainerRelation(rte))
	{
		/*
		 * We got here by planning the query part that needs to be executed on the query
		 * coordinator node.
		 * We have verified the occurrence of the citus_extra_datacontainer function
		 * encoding the remote scan we plan to execute here. We will replace all paths
		 * with a path describing our custom scan.
		 */
		Path *path = CreateCitusCustomScanPath(root, relOptInfo, restrictionIndex, rte,
											   ReplaceCitusExtraDataContainerWithCustomScan);

		/* replace all paths with our custom scan and recalculate cheapest */
		relOptInfo->pathlist = list_make1(path);
		set_cheapest(relOptInfo);

		return;
	}

	AdjustReadIntermediateResultCost(rte, relOptInfo);
	AdjustReadIntermediateResultArrayCost(rte, relOptInfo);

	if (rte->rtekind != RTE_RELATION)
	{
		return;
	}

	/*
	 * Use a memory context that's guaranteed to live long enough, could be
	 * called in a more shortly lived one (e.g. with GEQO).
	 */
	PlannerRestrictionContext *plannerRestrictionContext =
		CurrentPlannerRestrictionContext();
	MemoryContext restrictionsMemoryContext = plannerRestrictionContext->memoryContext;
	MemoryContext oldMemoryContext = MemoryContextSwitchTo(restrictionsMemoryContext);

	bool distributedTable = IsCitusTable(rte->relid);

	RelationRestriction *relationRestriction = palloc0(sizeof(RelationRestriction));
	relationRestriction->index = restrictionIndex;
	relationRestriction->relationId = rte->relid;
	relationRestriction->rte = rte;
	relationRestriction->relOptInfo = relOptInfo;
	relationRestriction->distributedRelation = distributedTable;
	relationRestriction->plannerInfo = root;

	/* see comments on GetVarFromAssignedParam() */
	relationRestriction->outerPlanParamsList = OuterPlanParamsList(root);
	relationRestriction->translatedVars = TranslatedVars(root,
														 relationRestriction->index);

	RelationRestrictionContext *relationRestrictionContext =
		plannerRestrictionContext->relationRestrictionContext;

	/*
	 * We're also keeping track of whether all participant
	 * tables are reference tables.
	 */
	if (distributedTable)
	{
		cacheEntry = GetCitusTableCacheEntry(rte->relid);

		relationRestrictionContext->allReferenceTables &=
			IsCitusTableTypeCacheEntry(cacheEntry, REFERENCE_TABLE);
	}

	relationRestrictionContext->relationRestrictionList =
		lappend(relationRestrictionContext->relationRestrictionList, relationRestriction);

	MemoryContextSwitchTo(oldMemoryContext);
}


/*
 * TranslatedVars deep copies the translated vars for the given relation index
 * if there is any append rel list.
 */
static List *
TranslatedVars(PlannerInfo *root, int relationIndex)
{
	List *translatedVars = NIL;

	if (root->append_rel_list != NIL)
	{
		AppendRelInfo *targetAppendRelInfo =
			FindTargetAppendRelInfo(root, relationIndex);
		if (targetAppendRelInfo != NULL)
		{
			/* postgres deletes translated_vars after pg13, hence we deep copy them here */
			Node *targetNode = NULL;
			foreach_ptr(targetNode, targetAppendRelInfo->translated_vars)
			{
				translatedVars =
					lappend(translatedVars, copyObject(targetNode));
			}
		}
	}
	return translatedVars;
}


/*
 * FindTargetAppendRelInfo finds the target append rel info for the given
 * relation rte index.
 */
static AppendRelInfo *
FindTargetAppendRelInfo(PlannerInfo *root, int relationRteIndex)
{
	AppendRelInfo *appendRelInfo = NULL;

	/* iterate on the queries that are part of UNION ALL subselects */
	foreach_ptr(appendRelInfo, root->append_rel_list)
	{
		/*
		 * We're only interested in the child rel that is equal to the
		 * relation we're investigating. Here we don't need to find the offset
		 * because postgres adds an offset to child_relid and parent_relid after
		 * calling multi_relation_restriction_hook.
		 */
		if (appendRelInfo->child_relid == relationRteIndex)
		{
			return appendRelInfo;
		}
	}
	return NULL;
}


/*
 * AdjustReadIntermediateResultCost adjusts the row count and total cost
 * of a read_intermediate_result call based on the file size.
 */
static void
AdjustReadIntermediateResultCost(RangeTblEntry *rangeTableEntry, RelOptInfo *relOptInfo)
{
	if (rangeTableEntry->rtekind != RTE_FUNCTION ||
		list_length(rangeTableEntry->functions) != 1)
	{
		/* avoid more expensive checks below for non-functions */
		return;
	}

	if (!CitusHasBeenLoaded() || !CheckCitusVersion(DEBUG5))
	{
		/* read_intermediate_result may not exist */
		return;
	}

	if (!ContainsReadIntermediateResultFunction((Node *) rangeTableEntry->functions))
	{
		return;
	}

	RangeTblFunction *rangeTableFunction = (RangeTblFunction *) linitial(
		rangeTableEntry->functions);
	FuncExpr *funcExpression = (FuncExpr *) rangeTableFunction->funcexpr;
	Const *resultIdConst = (Const *) linitial(funcExpression->args);
	if (!IsA(resultIdConst, Const))
	{
		/* not sure how to interpret non-const */
		return;
	}

	Datum resultIdDatum = resultIdConst->constvalue;

	Const *resultFormatConst = (Const *) lsecond(funcExpression->args);
	if (!IsA(resultFormatConst, Const))
	{
		/* not sure how to interpret non-const */
		return;
	}

	AdjustReadIntermediateResultsCostInternal(relOptInfo,
											  rangeTableFunction->funccoltypes,
											  1, &resultIdDatum, resultFormatConst);
}


/*
 * AdjustReadIntermediateResultArrayCost adjusts the row count and total cost
 * of a read_intermediate_results(resultIds, format) call based on the file size.
 */
static void
AdjustReadIntermediateResultArrayCost(RangeTblEntry *rangeTableEntry,
									  RelOptInfo *relOptInfo)
{
	Datum *resultIdArray = NULL;
	int resultIdCount = 0;

	if (rangeTableEntry->rtekind != RTE_FUNCTION ||
		list_length(rangeTableEntry->functions) != 1)
	{
		/* avoid more expensive checks below for non-functions */
		return;
	}

	if (!CitusHasBeenLoaded() || !CheckCitusVersion(DEBUG5))
	{
		/* read_intermediate_result may not exist */
		return;
	}

	if (!ContainsReadIntermediateResultArrayFunction((Node *) rangeTableEntry->functions))
	{
		return;
	}

	RangeTblFunction *rangeTableFunction =
		(RangeTblFunction *) linitial(rangeTableEntry->functions);
	FuncExpr *funcExpression = (FuncExpr *) rangeTableFunction->funcexpr;
	Const *resultIdConst = (Const *) linitial(funcExpression->args);
	if (!IsA(resultIdConst, Const))
	{
		/* not sure how to interpret non-const */
		return;
	}

	Datum resultIdArrayDatum = resultIdConst->constvalue;
	deconstruct_array(DatumGetArrayTypeP(resultIdArrayDatum), TEXTOID, -1, false,
					  'i', &resultIdArray, NULL, &resultIdCount);

	Const *resultFormatConst = (Const *) lsecond(funcExpression->args);
	if (!IsA(resultFormatConst, Const))
	{
		/* not sure how to interpret non-const */
		return;
	}

	AdjustReadIntermediateResultsCostInternal(relOptInfo,
											  rangeTableFunction->funccoltypes,
											  resultIdCount, resultIdArray,
											  resultFormatConst);
}


/*
 * AdjustReadIntermediateResultsCostInternal adjusts the row count and total cost
 * of reading intermediate results based on file sizes.
 */
static void
AdjustReadIntermediateResultsCostInternal(RelOptInfo *relOptInfo, List *columnTypes,
										  int resultIdCount, Datum *resultIds,
										  Const *resultFormatConst)
{
	PathTarget *reltarget = relOptInfo->reltarget;
	List *pathList = relOptInfo->pathlist;
	Path *path = NULL;
	double rowCost = 0.;
	double rowSizeEstimate = 0;
	double rowCountEstimate = 0.;
	double ioCost = 0.;
#if PG_VERSION_NUM >= PG_VERSION_12
	QualCost funcCost = { 0., 0. };
#else
	double funcCost = 0.;
#endif
	int64 totalResultSize = 0;
	ListCell *typeCell = NULL;

	Datum resultFormatDatum = resultFormatConst->constvalue;
	Oid resultFormatId = DatumGetObjectId(resultFormatDatum);
	bool binaryFormat = (resultFormatId == BinaryCopyFormatId());

	for (int index = 0; index < resultIdCount; index++)
	{
		char *resultId = TextDatumGetCString(resultIds[index]);
		int64 resultSize = IntermediateResultSize(resultId);
		if (resultSize < 0)
		{
			/* result does not exist, will probably error out later on */
			return;
		}

		if (binaryFormat)
		{
			/* subtract 11-byte signature + 8 byte header + 2-byte footer */
			totalResultSize -= 21;
		}

		totalResultSize += resultSize;
	}

	/* start with the cost of evaluating quals */
	rowCost += relOptInfo->baserestrictcost.per_tuple;

	/* postgres' estimate for the width of the rows */
	rowSizeEstimate += reltarget->width;

	/* add 2 bytes for column count (binary) or line separator (text) */
	rowSizeEstimate += 2;

	foreach(typeCell, columnTypes)
	{
		Oid columnTypeId = lfirst_oid(typeCell);
		Oid inputFunctionId = InvalidOid;
		Oid typeIOParam = InvalidOid;

		if (binaryFormat)
		{
			getTypeBinaryInputInfo(columnTypeId, &inputFunctionId, &typeIOParam);

			/* binary format: 4 bytes for field size */
			rowSizeEstimate += 4;
		}
		else
		{
			getTypeInputInfo(columnTypeId, &inputFunctionId, &typeIOParam);

			/* text format: 1 byte for tab separator */
			rowSizeEstimate += 1;
		}


		/* add the cost of parsing a column */
#if PG_VERSION_NUM >= PG_VERSION_12
		add_function_cost(NULL, inputFunctionId, NULL, &funcCost);
#else
		funcCost += get_func_cost(inputFunctionId);
#endif
	}
#if PG_VERSION_NUM >= PG_VERSION_12
	rowCost += funcCost.per_tuple;
#else
	rowCost += funcCost * cpu_operator_cost;
#endif

	/* estimate the number of rows based on the file size and estimated row size */
	rowCountEstimate = Max(1, (double) totalResultSize / rowSizeEstimate);

	/* cost of reading the data */
	ioCost = seq_page_cost * totalResultSize / BLCKSZ;

	Assert(pathList != NIL);

	/* tell the planner about the cost and row count of the function */
	path = (Path *) linitial(pathList);
	path->rows = rowCountEstimate;
	path->total_cost = rowCountEstimate * rowCost + ioCost;

#if PG_VERSION_NUM >= PG_VERSION_12
	path->startup_cost = funcCost.startup + relOptInfo->baserestrictcost.startup;
#endif
}


/*
 * OuterPlanParamsList creates a list of RootPlanParams for outer nodes of the
 * given root. The first item in the list corresponds to parent_root, and the
 * last item corresponds to the outer most node.
 */
static List *
OuterPlanParamsList(PlannerInfo *root)
{
	List *planParamsList = NIL;

	for (PlannerInfo *outerNodeRoot = root->parent_root; outerNodeRoot != NULL;
		 outerNodeRoot = outerNodeRoot->parent_root)
	{
		RootPlanParams *rootPlanParams = palloc0(sizeof(RootPlanParams));
		rootPlanParams->root = outerNodeRoot;

		/*
		 * TODO: In SearchPlannerParamList() we are only interested in Var plan
		 * params, consider copying just them here.
		 */
		rootPlanParams->plan_params = CopyPlanParamList(outerNodeRoot->plan_params);

		planParamsList = lappend(planParamsList, rootPlanParams);
	}

	return planParamsList;
}


/*
 * CopyPlanParamList deep copies the input PlannerParamItem list and returns the newly
 * allocated list.
 * Note that we cannot use copyObject() function directly since there is no support for
 * copying PlannerParamItem structs.
 */
static List *
CopyPlanParamList(List *originalPlanParamList)
{
	ListCell *planParamCell = NULL;
	List *copiedPlanParamList = NIL;

	foreach(planParamCell, originalPlanParamList)
	{
		PlannerParamItem *originalParamItem = lfirst(planParamCell);
		PlannerParamItem *copiedParamItem = makeNode(PlannerParamItem);

		copiedParamItem->paramId = originalParamItem->paramId;
		copiedParamItem->item = copyObject(originalParamItem->item);

		copiedPlanParamList = lappend(copiedPlanParamList, copiedParamItem);
	}

	return copiedPlanParamList;
}


/*
 * CreateAndPushPlannerRestrictionContext creates a new relation restriction context
 * and a new join context, inserts it to the beginning of the
 * plannerRestrictionContextList. Finally, the planner restriction context is
 * inserted to the beginning of the plannerRestrictionContextList and it is returned.
 */
static PlannerRestrictionContext *
CreateAndPushPlannerRestrictionContext(void)
{
	PlannerRestrictionContext *plannerRestrictionContext =
		palloc0(sizeof(PlannerRestrictionContext));

	plannerRestrictionContext->relationRestrictionContext =
		palloc0(sizeof(RelationRestrictionContext));

	plannerRestrictionContext->joinRestrictionContext =
		palloc0(sizeof(JoinRestrictionContext));

	plannerRestrictionContext->fastPathRestrictionContext =
		palloc0(sizeof(FastPathRestrictionContext));

	plannerRestrictionContext->memoryContext = CurrentMemoryContext;

	/* we'll apply logical AND as we add tables */
	plannerRestrictionContext->relationRestrictionContext->allReferenceTables = true;

	plannerRestrictionContextList = lcons(plannerRestrictionContext,
										  plannerRestrictionContextList);

	return plannerRestrictionContext;
}


/*
 * TranslatedVarsForRteIdentity gets an rteIdentity and returns the
 * translatedVars that belong to the range table relation. If no
 * translatedVars found, the function returns NIL;
 */
List *
TranslatedVarsForRteIdentity(int rteIdentity)
{
	PlannerRestrictionContext *currentPlannerRestrictionContext =
		CurrentPlannerRestrictionContext();

	List *relationRestrictionList =
		currentPlannerRestrictionContext->relationRestrictionContext->
		relationRestrictionList;
	RelationRestriction *relationRestriction = NULL;
	foreach_ptr(relationRestriction, relationRestrictionList)
	{
		if (GetRTEIdentity(relationRestriction->rte) == rteIdentity)
		{
			return relationRestriction->translatedVars;
		}
	}

	return NIL;
}


/*
 * CurrentRestrictionContext returns the most recently added
 * PlannerRestrictionContext from the plannerRestrictionContextList list.
 */
static PlannerRestrictionContext *
CurrentPlannerRestrictionContext(void)
{
	Assert(plannerRestrictionContextList != NIL);

	PlannerRestrictionContext *plannerRestrictionContext =
		(PlannerRestrictionContext *) linitial(plannerRestrictionContextList);

	if (plannerRestrictionContext == NULL)
	{
		ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR),
						errmsg("planner restriction context stack was empty"),
						errdetail("Please report this to the Citus core team.")));
	}

	return plannerRestrictionContext;
}


/*
 * PopPlannerRestrictionContext removes the most recently added restriction contexts from
 * the planner restriction context list. The function assumes the list is not empty.
 */
static void
PopPlannerRestrictionContext(void)
{
	plannerRestrictionContextList = list_delete_first(plannerRestrictionContextList);
}


/*
 * ResetPlannerRestrictionContext resets the element of the given planner
 * restriction context.
 */
static void
ResetPlannerRestrictionContext(PlannerRestrictionContext *plannerRestrictionContext)
{
	plannerRestrictionContext->relationRestrictionContext =
		palloc0(sizeof(RelationRestrictionContext));

	plannerRestrictionContext->joinRestrictionContext =
		palloc0(sizeof(JoinRestrictionContext));

	plannerRestrictionContext->fastPathRestrictionContext =
		palloc0(sizeof(FastPathRestrictionContext));


	/* we'll apply logical AND as we add tables */
	plannerRestrictionContext->relationRestrictionContext->allReferenceTables = true;
}


/*
 * HasUnresolvedExternParamsWalker returns true if the passed in expression
 * has external parameters that are not contained in boundParams, false
 * otherwise.
 */
bool
HasUnresolvedExternParamsWalker(Node *expression, ParamListInfo boundParams)
{
	if (expression == NULL)
	{
		return false;
	}

	if (IsA(expression, Param))
	{
		Param *param = (Param *) expression;
		int paramId = param->paramid;

		/* only care about user supplied parameters */
		if (param->paramkind != PARAM_EXTERN)
		{
			return false;
		}

		/* check whether parameter is available (and valid) */
		if (boundParams && paramId > 0 && paramId <= boundParams->numParams)
		{
			ParamExternData *externParam = NULL;

			/* give hook a chance in case parameter is dynamic */
			if (boundParams->paramFetch != NULL)
			{
				ParamExternData externParamPlaceholder;
				externParam = (*boundParams->paramFetch)(boundParams, paramId, false,
														 &externParamPlaceholder);
			}
			else
			{
				externParam = &boundParams->params[paramId - 1];
			}

			Oid paramType = externParam->ptype;
			if (OidIsValid(paramType))
			{
				return false;
			}
		}

		return true;
	}

	/* keep traversing */
	if (IsA(expression, Query))
	{
		return query_tree_walker((Query *) expression,
								 HasUnresolvedExternParamsWalker,
								 boundParams,
								 0);
	}
	else
	{
		return expression_tree_walker(expression,
									  HasUnresolvedExternParamsWalker,
									  boundParams);
	}
}


/*
 * GetRTEListPropertiesForQuery is a wrapper around GetRTEListProperties that
 * returns RTEListProperties for the rte list retrieved from query.
 */
RTEListProperties *
GetRTEListPropertiesForQuery(Query *query)
{
	List *rteList = ExtractRangeTableEntryList(query);
	return GetRTEListProperties(rteList);
}


/*
 * GetRTEListProperties returns RTEListProperties struct processing the given
 * rangeTableList.
 */
static RTEListProperties *
GetRTEListProperties(List *rangeTableList)
{
	RTEListProperties *rteListProperties = palloc0(sizeof(RTEListProperties));

	RangeTblEntry *rangeTableEntry = NULL;
	foreach_ptr(rangeTableEntry, rangeTableList)
	{
		if (rangeTableEntry->rtekind != RTE_RELATION)
		{
			continue;
		}
		else if (rangeTableEntry->relkind == RELKIND_VIEW)
		{
			/*
			 * Skip over views, distributed tables within (regular) views are
			 * already in rangeTableList.
			 */
			continue;
		}


		if (rangeTableEntry->relkind == RELKIND_MATVIEW)
		{
			/*
			 * Record materialized views as they are similar to postgres local tables
			 * but it is nice to record them separately.
			 *
			 * Regular tables, partitioned tables or foreign tables can be a local or
			 * distributed tables and we can qualify them accurately.
			 *
			 * For regular views, we don't care because their definitions are already
			 * in the same query tree and we can detect what is inside the view definition.
			 *
			 * For materialized views, they are just local tables in the queries. But, when
			 * REFRESH MATERIALIZED VIEW is used, they behave similar to regular views, adds
			 * the view definition to the query. Hence, it is useful to record it seperately
			 * and let the callers decide on what to do.
			 */
			rteListProperties->hasMaterializedView = true;
			continue;
		}

		Oid relationId = rangeTableEntry->relid;
		CitusTableCacheEntry *cacheEntry = LookupCitusTableCacheEntry(relationId);
		if (!cacheEntry)
		{
			rteListProperties->hasPostgresLocalTable = true;
		}
		else if (IsCitusTableTypeCacheEntry(cacheEntry, REFERENCE_TABLE))
		{
			rteListProperties->hasReferenceTable = true;
		}
		else if (IsCitusTableTypeCacheEntry(cacheEntry, CITUS_LOCAL_TABLE))
		{
			rteListProperties->hasCitusLocalTable = true;
		}
		else if (IsCitusTableTypeCacheEntry(cacheEntry, DISTRIBUTED_TABLE))
		{
			rteListProperties->hasDistributedTable = true;
		}
		else
		{
			/* it's not expected, but let's do a bug catch here */
			ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR),
							errmsg("encountered with an unexpected citus "
								   "table type while processing range table "
								   "entries of query")));
		}
	}

	rteListProperties->hasCitusTable = (rteListProperties->hasDistributedTable ||
										rteListProperties->hasReferenceTable ||
										rteListProperties->hasCitusLocalTable);

	return rteListProperties;
}
