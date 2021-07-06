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
#include <pthread.h>
#include <unistd.h>
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
typedef struct runSubPlanParallelPara {
	DistributedSubPlan *subPlan;
	HTAB *intermediateResultsHash;
	uint64 planId;
}runSubPlanParallelPara;

void runSubPlanParallel(void *arg) {
	ereport(DEBUG3, (errmsg("$$$$$$$$$$$$$$$$$$walk into runSubPlanParallel")));
	runSubPlanParallelPara *para;
	para = (runSubPlanParallelPara *) arg;
	DistributedSubPlan *subPlan = para->subPlan;
	ereport(DEBUG3, (errmsg("$$$$$$$$$$$$$$$$$$walk into runSubPlanParallel subPlan:%p" ,subPlan)));
	HTAB *intermediateResultsHash = para->intermediateResultsHash;
	uint64 planId = para->planId;
	ereport(DEBUG3, (errmsg("$$$$$$$$$$$$$$$$$$runSubPlanParallel planId:%d" ,planId)));
	long start_time = getTimeUsec()/1000;
	PlannedStmt *plannedStmt = subPlan->plan;
	uint32 subPlanId = subPlan->subPlanId;
	ereport(DEBUG3, (errmsg("$$$$$$$$$$$$$$$$$$runSubPlanParallel subPlanId:%d" ,subPlanId)));
	ParamListInfo params = NULL;
	char *resultId = GenerateResultId(planId, subPlanId);
	if (IsLoggableLevel(DEBUG3)) {
		ereport(DEBUG3, (errmsg("$$$$$$$$$$$$$$$$$$runSubPlanParallel resultId:%s, plannedStmt:%p" ,resultId,plannedStmt)));
		elog_node_display(LOG, "plannedStmt parse tree", plannedStmt, Debug_pretty_print);
		sleep(2);
	}
	ereport(DEBUG3, (errmsg("$$$$$$$$$$$$$$$$$$ STEP 1" )));
	List *remoteWorkerNodeList =
		FindAllWorkerNodesUsingSubplan(intermediateResultsHash, resultId);
	ereport(DEBUG3, (errmsg("$$$$$$$$$$$$$$$$$$ STEP 2" )));
	IntermediateResultsHashEntry *entry =
		SearchIntermediateResult(intermediateResultsHash, resultId);
	SubPlanLevel++;
	ereport(DEBUG3, (errmsg("$$$$$$$$$$$$$$$$$$ STEP 3" )));
	EState *estate = CreateExecutorState();
	ereport(DEBUG3, (errmsg("$$$$$$$$$$$$$$$$$$ STEP 4" )));
	DestReceiver *copyDest =
		CreateRemoteFileDestReceiver(resultId, estate, remoteWorkerNodeList,
									 entry->writeLocalFile);
	ereport(DEBUG3, (errmsg("$$$$$$$$$$$$$$$$$$ STEP 5" )));
	TimestampTz startTimestamp = GetCurrentTimestamp();
	ereport(DEBUG3, (errmsg("$$$$$$$$$$$$$$$$$$ STEP 6" )));
	ExecutePlanIntoDestReceiver(plannedStmt, params, copyDest);
	ereport(DEBUG3, (errmsg("$$$$$$$$$$$$$$$$$$ STEP 7" )));
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

	/* ------------- danny test begin ---------------  */
	DistributedSubPlan *subPlan = NULL;
	ereport(DEBUG3, (errmsg("$$$$$$$$$$$$$$$$$$outer planId:%d" ,planId)));
	int index = 1; 
	pthread_t thrd1, thrd2, thrd3;
	runSubPlanParallelPara para1,para2,para3;
	foreach_ptr(subPlan, subPlanList){
		PlannedStmt *plannedStmt = subPlan->plan;
		ereport(DEBUG3, (errmsg("$$$$$$$$$$$$$$$$$$ subPlan:%p, plannedStmt:%p, index:%d" ,subPlan, plannedStmt, index)));
		if (index == 1) {
			para1.subPlan = subPlan;
			para1.intermediateResultsHash = intermediateResultsHash;
			para1.planId = planId;
			pthread_create(&thrd1, NULL, (void *)runSubPlanParallel, &para1);
			index++;
			sleep(5);
			continue;
		} else if (index == 2) {
			para2.subPlan = subPlan;
			para2.intermediateResultsHash = intermediateResultsHash;
			para2.planId = planId;
			pthread_create(&thrd2, NULL, (void *)runSubPlanParallel, &para2);
			index++;
			sleep(1);
			continue;
		} else if (index == 3) {
			para3.subPlan = subPlan;
			para3.intermediateResultsHash = intermediateResultsHash;
			para3.planId = planId;
			pthread_create(&thrd3, NULL, (void *)runSubPlanParallel, &para3);
			index++;
			sleep(1);
			continue;
		} else if (index == 6) {
			pthread_join(thrd1, NULL);
			pthread_join(thrd2, NULL);
			pthread_join(thrd3, NULL);
			ereport(DEBUG3, (errmsg("$$$$$$$$$$$$$$$$$$  ready to run fulfill")));
			sleep(20);
		}
		index++;



		// /* ------------- danny test begin ---------------  */
		// long start_time = getTimeUsec()/1000;
		// /* ------------- danny test end ---------------  */
		// PlannedStmt *plannedStmt = subPlan->plan;
		// uint32 subPlanId = subPlan->subPlanId;
		// ParamListInfo params = NULL;
		// char *resultId = GenerateResultId(planId, subPlanId);
		// /* ------------- danny test begin ---------------  */
		// if (IsLoggableLevel(DEBUG3)) {
		// 	ereport(DEBUG3, (errmsg("$$$$$$$$$$$$$$$$$$resultId:%s" ,resultId)));
		// 	//elog_node_display(LOG, "plannedStmt parse tree", plannedStmt, Debug_pretty_print);
		// }
		// /* ------------- danny test end ---------------  */
		// List *remoteWorkerNodeList =
		// 	FindAllWorkerNodesUsingSubplan(intermediateResultsHash, resultId);

		// IntermediateResultsHashEntry *entry =
		// 	SearchIntermediateResult(intermediateResultsHash, resultId);

		// SubPlanLevel++;
		// EState *estate = CreateExecutorState();
		// DestReceiver *copyDest =
		// 	CreateRemoteFileDestReceiver(resultId, estate, remoteWorkerNodeList,
		// 								 entry->writeLocalFile);

		// TimestampTz startTimestamp = GetCurrentTimestamp();

		// ExecutePlanIntoDestReceiver(plannedStmt, params, copyDest);

		// /*
		//  * EXPLAIN ANALYZE instrumentations. Calculating these are very light-weight,
		//  * so always populate them regardless of EXPLAIN ANALYZE or not.
		//  */
		// long durationSeconds = 0.0;
		// int durationMicrosecs = 0;
		// TimestampDifference(startTimestamp, GetCurrentTimestamp(), &durationSeconds,
		// 					&durationMicrosecs);

		// subPlan->durationMillisecs = durationSeconds * SECOND_TO_MILLI_SECOND;
		// subPlan->durationMillisecs += durationMicrosecs * MICRO_TO_MILLI_SECOND;
		// /* ------------- danny test begin ---------------  */
		// ereport(DEBUG3, (errmsg("$$$$$$$$$$$$$$$$$$resultId:%s,start_time:%d,run_duration:%f" ,resultId,start_time, subPlan->durationMillisecs)));
		// /* ------------- danny test end ---------------  */
		// subPlan->bytesSentPerWorker = RemoteFileDestReceiverBytesSent(copyDest);
		// subPlan->remoteWorkerCount = list_length(remoteWorkerNodeList);
		// subPlan->writeLocalFile = entry->writeLocalFile;

		// SubPlanLevel--;
		// FreeExecutorState(estate);
		// index++;
	}
	sleep(20);
	return;
	/* ------------- danny test end ---------------  */


	// DistributedSubPlan *subPlan = NULL;
	// foreach_ptr(subPlan, subPlanList)
	// {
	// 	/* ------------- danny test begin ---------------  */
	// 	long start_time = getTimeUsec()/1000;
	// 	/* ------------- danny test end ---------------  */
	// 	PlannedStmt *plannedStmt = subPlan->plan;
	// 	uint32 subPlanId = subPlan->subPlanId;
	// 	ParamListInfo params = NULL;
	// 	char *resultId = GenerateResultId(planId, subPlanId);
	// 	/* ------------- danny test begin ---------------  */
	// 	if (IsLoggableLevel(DEBUG3)) {
	// 		ereport(DEBUG3, (errmsg("$$$$$$$$$$$$$$$$$$resultId:%s" ,resultId)));
	// 		//elog_node_display(LOG, "plannedStmt parse tree", plannedStmt, Debug_pretty_print);
	// 	}
	// 	/* ------------- danny test end ---------------  */
	// 	List *remoteWorkerNodeList =
	// 		FindAllWorkerNodesUsingSubplan(intermediateResultsHash, resultId);

	// 	IntermediateResultsHashEntry *entry =
	// 		SearchIntermediateResult(intermediateResultsHash, resultId);

	// 	SubPlanLevel++;
	// 	EState *estate = CreateExecutorState();
	// 	DestReceiver *copyDest =
	// 		CreateRemoteFileDestReceiver(resultId, estate, remoteWorkerNodeList,
	// 									 entry->writeLocalFile);

	// 	TimestampTz startTimestamp = GetCurrentTimestamp();

	// 	ExecutePlanIntoDestReceiver(plannedStmt, params, copyDest);

	// 	/*
	// 	 * EXPLAIN ANALYZE instrumentations. Calculating these are very light-weight,
	// 	 * so always populate them regardless of EXPLAIN ANALYZE or not.
	// 	 */
	// 	long durationSeconds = 0.0;
	// 	int durationMicrosecs = 0;
	// 	TimestampDifference(startTimestamp, GetCurrentTimestamp(), &durationSeconds,
	// 						&durationMicrosecs);

	// 	subPlan->durationMillisecs = durationSeconds * SECOND_TO_MILLI_SECOND;
	// 	subPlan->durationMillisecs += durationMicrosecs * MICRO_TO_MILLI_SECOND;
	// 	/* ------------- danny test begin ---------------  */
	// 	ereport(DEBUG3, (errmsg("$$$$$$$$$$$$$$$$$$resultId:%s,start_time:%d,run_duration:%f" ,resultId,start_time, subPlan->durationMillisecs)));
	// 	/* ------------- danny test end ---------------  */
	// 	subPlan->bytesSentPerWorker = RemoteFileDestReceiverBytesSent(copyDest);
	// 	subPlan->remoteWorkerCount = list_length(remoteWorkerNodeList);
	// 	subPlan->writeLocalFile = entry->writeLocalFile;

	// 	SubPlanLevel--;
	// 	FreeExecutorState(estate);
	// }
}
