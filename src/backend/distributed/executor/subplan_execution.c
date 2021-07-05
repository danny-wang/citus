/*-------------------------------------------------------------------------
 *
 * subplan_execution.c
 *
 * Functions for execution subplans prior to distributed table execution.
 *
 * Copyright (c) Citus Data, Inc.
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "distributed/intermediate_result_pruning.h"
#include "distributed/intermediate_results.h"
#include "distributed/listutils.h"
#include "distributed/multi_executor.h"
#include "distributed/multi_physical_planner.h"
#include "distributed/recursive_planning.h"
#include "distributed/subplan_execution.h"
#include "distributed/transaction_management.h"
#include "distributed/worker_manager.h"
#include "executor/executor.h"
#include "utils/datetime.h"
/* ------------- danny test begin ---------------  */
#include "nodes/print.h"
#include <time.h>
#include <sys/time.h>
/* ------------- danny test end ---------------  */
#define SECOND_TO_MILLI_SECOND 1000
#define MICRO_TO_MILLI_SECOND 0.001

int MaxIntermediateResult = 1048576; /* maximum size in KB the intermediate result can grow to */
/* when this is true, we enforce intermediate result size limit in all executors */
int SubPlanLevel = 0;

/* ------------- danny test begin ---------------  */
long getTimeUsec()
{
    struct timeval t;
    gettimeofday(&t, 0);
    return (long)((long)t.tv_sec * 1000 * 1000 + t.tv_usec);
}
/* ------------- danny test end ---------------  */

/*
 * ExecuteSubPlans executes a list of subplans from a distributed plan
 * by sequentially executing each plan from the top.
 */
void
ExecuteSubPlans(DistributedPlan *distributedPlan)
{
	uint64 planId = distributedPlan->planId;
	List *subPlanList = distributedPlan->subPlanList;

	if (subPlanList == NIL)
	{
		/* no subplans to execute */
		return;
	}

	HTAB *intermediateResultsHash = MakeIntermediateResultHTAB();
	RecordSubplanExecutionsOnNodes(intermediateResultsHash, distributedPlan);

	/*
	 * Make sure that this transaction has a distributed transaction ID.
	 *
	 * Intermediate results of subplans will be stored in a directory that is
	 * derived from the distributed transaction ID.
	 */
	UseCoordinatedTransaction();

	DistributedSubPlan *subPlan = NULL;
	foreach_ptr(subPlan, subPlanList)
	{
		/* ------------- danny test begin ---------------  */
		long start_time = getTimeUsec()/1000;
		/* ------------- danny test end ---------------  */
		PlannedStmt *plannedStmt = subPlan->plan;
		uint32 subPlanId = subPlan->subPlanId;
		ParamListInfo params = NULL;
		char *resultId = GenerateResultId(planId, subPlanId);
		/* ------------- danny test begin ---------------  */
		if (IsLoggableLevel(DEBUG3)) {
			ereport(DEBUG3, (errmsg("$$$$$$$$$$$$$$$$$$resultId:%s" ,resultId)));
			//elog_node_display(LOG, "plannedStmt parse tree", plannedStmt, Debug_pretty_print);
		}
		/* ------------- danny test end ---------------  */
		List *remoteWorkerNodeList =
			FindAllWorkerNodesUsingSubplan(intermediateResultsHash, resultId);

		IntermediateResultsHashEntry *entry =
			SearchIntermediateResult(intermediateResultsHash, resultId);

		SubPlanLevel++;
		EState *estate = CreateExecutorState();
		DestReceiver *copyDest =
			CreateRemoteFileDestReceiver(resultId, estate, remoteWorkerNodeList,
										 entry->writeLocalFile);

		TimestampTz startTimestamp = GetCurrentTimestamp();

		ExecutePlanIntoDestReceiver(plannedStmt, params, copyDest);

		/*
		 * EXPLAIN ANALYZE instrumentations. Calculating these are very light-weight,
		 * so always populate them regardless of EXPLAIN ANALYZE or not.
		 */
		long durationSeconds = 0.0;
		int durationMicrosecs = 0;
		TimestampDifference(startTimestamp, GetCurrentTimestamp(), &durationSeconds,
							&durationMicrosecs);

		subPlan->durationMillisecs = durationSeconds * SECOND_TO_MILLI_SECOND;
		subPlan->durationMillisecs += durationMicrosecs * MICRO_TO_MILLI_SECOND;
		/* ------------- danny test begin ---------------  */
		ereport(DEBUG3, (errmsg("$$$$$$$$$$$$$$$$$$resultId:%s,start_time:%d,run_duration:%f" ,resultId,start_time, subPlan->durationMillisecs)));
		/* ------------- danny test end ---------------  */
		subPlan->bytesSentPerWorker = RemoteFileDestReceiverBytesSent(copyDest);
		subPlan->remoteWorkerCount = list_length(remoteWorkerNodeList);
		subPlan->writeLocalFile = entry->writeLocalFile;

		SubPlanLevel--;
		FreeExecutorState(estate);
	}
}
