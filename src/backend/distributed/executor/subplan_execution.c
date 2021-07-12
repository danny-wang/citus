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
#include "libpq/libpq.h"
#include "libpq/pqformat.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#if PG_VERSION_NUM >= PG_VERSION_13
#include "tcop/cmdtag.h"
#endif
#include "postgres.h"
#include "libpq-fe.h"
#include "libpq-int.h"
#include "fmgr.h"
#include "pqexpbuffer.h"
#include "distributed/connection_management.h"
#include "distributed/adaptive_executor.h"
#include "distributed/transmit.h"
#include "nodes/print.h"
#include <time.h>
#include <sys/time.h>
#include <pthread.h>
#include <unistd.h>
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/syscache.h"
#include "utils/memutils.h"
#include <stdio.h>
/* ------------- danny test end ---------------  */
#define SECOND_TO_MILLI_SECOND 1000
#define MICRO_TO_MILLI_SECOND 0.001

int MaxIntermediateResult = 1048576; /* maximum size in KB the intermediate result can grow to */
/* when this is true, we enforce intermediate result size limit in all executors */
int SubPlanLevel = 0;

/* ------------- danny test begin ---------------  */
static const char BinarySignature[11] = "PGCOPY\n\377\r\n\0";
const int32 zero = 0;
long getTimeUsec()
{
    struct timeval t;
    gettimeofday(&t, 0);
    return (long)((long)t.tv_sec * 1000 * 1000 + t.tv_usec);
}
// typedef struct runSubPlanParallelPara {
// 	DistributedSubPlan *subPlan;
// 	HTAB *intermediateResultsHash;
// 	uint64 planId;
// }runSubPlanParallelPara;

// void runSubPlanParallel(void *arg) {
// 	ereport(DEBUG3, (errmsg("$$$$$$$$$$$$$$$$$$walk into runSubPlanParallel")));
// 	runSubPlanParallelPara *para;
// 	para = (runSubPlanParallelPara *) arg;
// 	DistributedSubPlan *subPlan = para->subPlan;
// 	ereport(DEBUG3, (errmsg("$$$$$$$$$$$$$$$$$$walk into runSubPlanParallel subPlan:%p" ,subPlan)));
// 	HTAB *intermediateResultsHash = para->intermediateResultsHash;
// 	uint64 planId = para->planId;
// 	ereport(DEBUG3, (errmsg("$$$$$$$$$$$$$$$$$$runSubPlanParallel planId:%d" ,planId)));
// 	long start_time = getTimeUsec()/1000;
// 	PlannedStmt *plannedStmt = subPlan->plan;
// 	uint32 subPlanId = subPlan->subPlanId;
// 	ereport(DEBUG3, (errmsg("$$$$$$$$$$$$$$$$$$runSubPlanParallel subPlanId:%d" ,subPlanId)));
// 	ParamListInfo params = NULL;
// 	char *resultId = GenerateResultId(planId, subPlanId);
// 	if (IsLoggableLevel(DEBUG3)) {
// 		ereport(DEBUG3, (errmsg("$$$$$$$$$$$$$$$$$$runSubPlanParallel resultId:%s, plannedStmt:%p" ,resultId,plannedStmt)));
// 		//elog_node_display(LOG, "plannedStmt parse tree", plannedStmt, Debug_pretty_print);
// 		//sleep(2);
// 	}
// 	ereport(DEBUG3, (errmsg("$$$$$$$$$$$$$$$$$$ STEP 1" )));
// 	List *remoteWorkerNodeList =
// 		FindAllWorkerNodesUsingSubplan(intermediateResultsHash, resultId);
// 	ereport(DEBUG3, (errmsg("$$$$$$$$$$$$$$$$$$ STEP 2" )));
// 	IntermediateResultsHashEntry *entry =
// 		SearchIntermediateResult(intermediateResultsHash, resultId);
// 	SubPlanLevel++;
// 	ereport(DEBUG3, (errmsg("$$$$$$$$$$$$$$$$$$ STEP 3" )));
// 	EState *estate = CreateExecutorState();
// 	ereport(DEBUG3, (errmsg("$$$$$$$$$$$$$$$$$$ STEP 4" )));
// 	DestReceiver *copyDest =
// 		CreateRemoteFileDestReceiver(resultId, estate, remoteWorkerNodeList,
// 									 entry->writeLocalFile);
// 	ereport(DEBUG3, (errmsg("$$$$$$$$$$$$$$$$$$ STEP 5" )));
// 	TimestampTz startTimestamp = GetCurrentTimestamp();
// 	ereport(DEBUG3, (errmsg("$$$$$$$$$$$$$$$$$$ STEP 6" )));
// 	ExecutePlanIntoDestReceiver(plannedStmt, params, copyDest);
// 	ereport(DEBUG3, (errmsg("$$$$$$$$$$$$$$$$$$ STEP 7" )));
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



/* Append data to the copy buffer in outputState */
static void
CopySendData(CopyOutState outputState, const void *databuf, int datasize)
{
	appendBinaryStringInfo(outputState->fe_msgbuf, databuf, datasize);
}

/* Append an int16 to the copy buffer in outputState. */
static void
CopySendInt16(CopyOutState outputState, int16 val)
{
	uint16 buf = htons((uint16) val);
	CopySendData(outputState, &buf, sizeof(buf));
}

/* Append an int32 to the copy buffer in outputState. */
static void
CopySendInt32(CopyOutState outputState, int32 val)
{
	uint32 buf = htonl((uint32) val);
	CopySendData(outputState, &buf, sizeof(buf));
}

/* *INDENT-ON* */
/* Helper function to send pending copy output */
static inline void
CopyFlushOutput(CopyOutState cstate, char *start, char *pointer)
{
	if (pointer > start)
	{
		CopySendData(cstate, start, pointer - start);
	}
}

/* Append a striong to the copy buffer in outputState. */
static void
CopySendString(CopyOutState outputState, const char *str)
{
	appendBinaryStringInfo(outputState->fe_msgbuf, str, strlen(str));
}

/* Append a char to the copy buffer in outputState. */
static void
CopySendChar(CopyOutState outputState, char c)
{
	appendStringInfoCharMacro(outputState->fe_msgbuf, c);
}

/*
 * TypeOutputFunctions takes an array of types and returns an array of output functions
 * for those types.
 */
static FmgrInfo *
TypeOutputFunctions(uint32 columnCount, Oid *typeIdArray, bool binaryFormat)
{
	FmgrInfo *columnOutputFunctions = palloc0(columnCount * sizeof(FmgrInfo));

	for (uint32 columnIndex = 0; columnIndex < columnCount; columnIndex++)
	{
		FmgrInfo *currentOutputFunction = &columnOutputFunctions[columnIndex];
		Oid columnTypeId = typeIdArray[columnIndex];
		bool typeVariableLength = false;
		Oid outputFunctionId = InvalidOid;

		if (columnTypeId == InvalidOid)
		{
			/* TypeArrayFromTupleDescriptor decided to skip this column */
			continue;
		}
		else if (binaryFormat)
		{
			getTypeBinaryOutputInfo(columnTypeId, &outputFunctionId, &typeVariableLength);
			ereport(DEBUG3, (errmsg("columnTypeId:%d binaryOutputFunctionId：%d",columnTypeId,outputFunctionId)));
		}
		else
		{
			getTypeOutputInfo(columnTypeId, &outputFunctionId, &typeVariableLength);
			ereport(DEBUG3, (errmsg("columnTypeId:%d outputFunctionId：%d",columnTypeId,outputFunctionId)));
		}

		fmgr_info(outputFunctionId, currentOutputFunction);
	}

	return columnOutputFunctions;
}

/*
 * Send text representation of one column, with conversion and escaping.
 *
 * NB: This function is based on commands/copy.c and doesn't fully conform to
 * our coding style. The function should be kept in sync with copy.c.
 */
static void
CopyAttributeOutText(CopyOutState cstate, char *string)
{
	char *pointer = NULL;
	char c = '\0';
	char delimc = cstate->delim[0];

	if (cstate->need_transcoding)
	{
		pointer = pg_server_to_any(string, strlen(string), cstate->file_encoding);
	}
	else
	{
		pointer = string;
	}

	/*
	 * We have to grovel through the string searching for control characters
	 * and instances of the delimiter character.  In most cases, though, these
	 * are infrequent.  To avoid overhead from calling CopySendData once per
	 * character, we dump out all characters between escaped characters in a
	 * single call.  The loop invariant is that the data from "start" to "pointer"
	 * can be sent literally, but hasn't yet been.
	 *
	 * As all encodings here are safe, i.e. backend supported ones, we can
	 * skip doing pg_encoding_mblen(), because in valid backend encodings,
	 * extra bytes of a multibyte character never look like ASCII.
	 */
	char *start = pointer;
	while ((c = *pointer) != '\0')
	{
		if ((unsigned char) c < (unsigned char) 0x20)
		{
			/*
			 * \r and \n must be escaped, the others are traditional. We
			 * prefer to dump these using the C-like notation, rather than
			 * a backslash and the literal character, because it makes the
			 * dump file a bit more proof against Microsoftish data
			 * mangling.
			 */
			switch (c)
			{
				case '\b':
					c = 'b';
					break;
				case '\f':
					c = 'f';
					break;
				case '\n':
					c = 'n';
					break;
				case '\r':
					c = 'r';
					break;
				case '\t':
					c = 't';
					break;
				case '\v':
					c = 'v';
					break;
				default:
					/* If it's the delimiter, must backslash it */
					if (c == delimc)
						break;
					/* All ASCII control chars are length 1 */
					pointer++;
					continue;		/* fall to end of loop */
			}
			/* if we get here, we need to convert the control char */
			CopyFlushOutput(cstate, start, pointer);
			CopySendChar(cstate, '\\');
			CopySendChar(cstate, c);
			start = ++pointer;	/* do not include char in next run */
		}
		else if (c == '\\' || c == delimc)
		{
			CopyFlushOutput(cstate, start, pointer);
			CopySendChar(cstate, '\\');
			start = pointer++;	/* we include char in next run */
		}
		else
		{
			pointer++;
		}
	}

	CopyFlushOutput(cstate, start, pointer);
}



typedef struct SubPlanParallel {
	DistributedSubPlan *subPlan;
	char *fileName;
	FileCompat fc;
	char *nodeName;
	uint32 nodePort;
	char *queryStringLazy;
	PGconn *conn;
} SubPlanParallel;


typedef struct SubPlanParallelExecution
{
	/*
	 * Flag to indiciate that the set of connections we are interested
	 * in has changed and waitEventSet needs to be rebuilt.
	 */
	bool rebuildWaitEventSet;
	/*
	 * Flag to indiciate that the set of wait events we are interested
	 * in might have changed and waitEventSet needs to be updated.
	 *
	 * Note that we set this flag whenever we assign a value to waitFlags,
	 * but we don't check that the waitFlags is actually different from the
	 * previous value. So we might have some false positives for this flag,
	 * which is OK, because in this case ModifyWaitEvent() is noop.
	 */
	bool waitFlagsChanged;
	/*
	 * WaitEventSet used for waiting for I/O events.
	 *
	 * This could also be local to RunDistributedExecution(), but in that case
	 * we had to mark it as "volatile" to avoid PG_TRY()/PG_CATCH() issues, and
	 * cast it to non-volatile when doing WaitEventSetFree(). We thought that
	 * would make code a bit harder to read than making this non-local, so we
	 * move it here. See comments for PG_TRY() in postgres/src/include/elog.h
	 * and "man 3 siglongjmp" for more context.
	 */
	WaitEventSet *waitEventSet;
	/* total number of tasks to execute */
	int totalTaskCount;

	/* number of tasks that still need to be executed */
	int unfinishedTaskCount;
	/*
	 * Flag to indicate whether throwing errors on cancellation is
	 * allowed.
	 */
	bool raiseInterrupts;
	/* indicates whether distributed execution has failed */
	bool failed;
	/*
	 * For SELECT commands or INSERT/UPDATE/DELETE commands with RETURNING,
	 * the total number of rows received from the workers. For
	 * INSERT/UPDATE/DELETE commands without RETURNING, the total number of
	 * tuples modified.
	 *
	 * Note that for replicated tables (e.g., reference tables), we only consider
	 * a single replica's rows that are processed.
	 */
	uint64 rowsProcessed;

	/*
	 * The following fields are used while receiving results from remote nodes.
	 * We store this information here to avoid re-allocating it every time.
	 *
	 * columnArray field is reset/calculated per row, so might be useless for
	 * other contexts. The benefit of keeping it here is to avoid allocating
	 * the array over and over again.
	 */
	uint32 allocatedColumnCount;
	void **columnArray;
	StringInfoData *stringInfoDataArray;

	/*
	 * jobIdList contains all jobs in the job tree, this is used to
	 * do cleanup for repartition queries.
	 */
	List *jobIdList;
};
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
	/* ------------- danny test begin ---------------  */
	elog_node_display(LOG, "distributedPlan parse tree", distributedPlan, Debug_pretty_print);

	ereport(DEBUG3, (errmsg("$$$$$$$$$$$$$$$$$$ walk into ExecuteSubPlanssubPlan")));
	ereport(DEBUG3, (errmsg("type:%d, planId:%d, modLevel:%d, expectResults:%d, queryId:%d" ,distributedPlan->type.citus_tag, distributedPlan->planId, distributedPlan->modLevel,
		distributedPlan->expectResults, distributedPlan->queryId)));
	if (distributedPlan->workerJob != NULL) {
		ereport(DEBUG3, (errmsg("job type:%d, jobId:%d, subqueryPushdown:%d, requiresCoordinatorEvaluation:%d, deferredPruning:%d" ,
			distributedPlan->workerJob->type.citus_tag, distributedPlan->workerJob->jobId, distributedPlan->workerJob->subqueryPushdown,
		distributedPlan->workerJob->requiresCoordinatorEvaluation,distributedPlan->workerJob->deferredPruning)));
		StringInfo subqueryString = makeStringInfo();
		pg_get_query_def(distributedPlan->workerJob->jobQuery, subqueryString);
		ereport(DEBUG3, (errmsg("jobQuery:%s",ApplyLogRedaction(subqueryString->data))));
		elog_node_display(LOG, "taskList parse tree", distributedPlan->workerJob->taskList, Debug_pretty_print);
		elog_node_display(LOG, "dependentJobList parse tree", distributedPlan->workerJob->dependentJobList, Debug_pretty_print);
		elog_node_display(LOG, "localPlannedStatements parse tree", distributedPlan->workerJob->localPlannedStatements, Debug_pretty_print);
		elog_node_display(LOG, "partitionKeyValue parse tree", distributedPlan->workerJob->partitionKeyValue, Debug_pretty_print);
	}
	elog_node_display(LOG, "combineQuery parse tree", distributedPlan->combineQuery, Debug_pretty_print);
	elog_node_display(LOG, "relationIdList parse tree", distributedPlan->relationIdList, Debug_pretty_print);
	elog_node_display(LOG, "insertSelectQuery parse tree", distributedPlan->insertSelectQuery, Debug_pretty_print);
	elog_node_display(LOG, "selectPlanForInsertSelect parse tree", distributedPlan->selectPlanForInsertSelect, Debug_pretty_print);
	if (distributedPlan->intermediateResultIdPrefix) {
		ereport(DEBUG3, (errmsg("insertSelectMethod:%d, intermediateResultIdPrefix:%s, fastPathRouterPlan:%d" ,distributedPlan->insertSelectMethod,
	 distributedPlan->intermediateResultIdPrefix, distributedPlan->fastPathRouterPlan)));
	} else {
		ereport(DEBUG3, (errmsg("insertSelectMethod:%d, fastPathRouterPlan:%d" ,distributedPlan->insertSelectMethod, distributedPlan->fastPathRouterPlan)));
	}
	
	elog_node_display(LOG, "subPlanList parse tree", distributedPlan->subPlanList, Debug_pretty_print);
	ereport(DEBUG3, (errmsg("usedSubPlanNodeList length:%d",list_length(distributedPlan->usedSubPlanNodeList))));
	elog_node_display(LOG, "usedSubPlanNodeList parse tree", distributedPlan->usedSubPlanNodeList, Debug_pretty_print);
	DistributedSubPlan *subPlan5 = NULL;
	ereport(DEBUG3, (errmsg("subPlanList length:%d",list_length(distributedPlan->subPlanList))));
	foreach_ptr(subPlan5, subPlanList)
	{
		PlannedStmt *plannedStmt = subPlan5->plan;
		elog_node_display(LOG, "plannedStmt parse tree", plannedStmt, Debug_pretty_print);
		elog_node_display(LOG, "plannedStmt rtable parse tree", plannedStmt->rtable, Debug_pretty_print);
		elog_node_display(LOG, "plannedStmt subplans parse tree", plannedStmt->subplans, Debug_pretty_print);
	}
	/* ------------- danny test begin ---------------  */

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
	// DistributedSubPlan *subPlan = NULL;
	// ereport(DEBUG3, (errmsg("$$$$$$$$$$$$$$$$$$outer planId:%d" ,planId)));
	// int index = 1; 
	// pthread_t thrd1, thrd2, thrd3;
	// runSubPlanParallelPara para1,para2,para3;
	// foreach_ptr(subPlan, subPlanList){
	// 	PlannedStmt *plannedStmt = subPlan->plan;
	// 	ereport(DEBUG3, (errmsg("$$$$$$$$$$$$$$$$$$ subPlan:%p, plannedStmt:%p, index:%d" ,subPlan, plannedStmt, index)));
	// 	if (index == 1) {
	// 		para1.subPlan = subPlan;
	// 		para1.intermediateResultsHash = intermediateResultsHash;
	// 		para1.planId = planId;
	// 		pthread_create(&thrd1, NULL, (void *)runSubPlanParallel, &para1);
	// 		index++;
	// 		sleep(5);
	// 		continue;
	// 	} else if (index == 2) {
	// 		para2.subPlan = subPlan;
	// 		para2.intermediateResultsHash = intermediateResultsHash;
	// 		para2.planId = planId;
	// 		pthread_create(&thrd2, NULL, (void *)runSubPlanParallel, &para2);
	// 		index++;
	// 		sleep(1);
	// 		continue;
	// 	} else if (index == 3) {
	// 		para3.subPlan = subPlan;
	// 		para3.intermediateResultsHash = intermediateResultsHash;
	// 		para3.planId = planId;
	// 		pthread_create(&thrd3, NULL, (void *)runSubPlanParallel, &para3);
	// 		index++;
	// 		sleep(1);
	// 		continue;
	// 	} else if (index == 6) {
	// 		pthread_join(thrd1, NULL);
	// 		pthread_join(thrd2, NULL);
	// 		pthread_join(thrd3, NULL);
	// 		ereport(DEBUG3, (errmsg("$$$$$$$$$$$$$$$$$$  ready to run fulfill")));
	// 		sleep(20);
	// 	}
	// 	index++;

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
	// }
	// sleep(20);
	// return;

	// 1. find all independent subplans
	const int fileFlags = (O_APPEND | O_CREAT | O_RDWR | O_TRUNC | PG_BINARY);
	const int fileMode = (S_IRUSR | S_IWUSR);
	const char *delimiterCharacter = "\t";
	const char *nullPrintCharacter = "\\N";
	List *sequenceJobList;
	List *parallelJobList;
	DistributedSubPlan *subPlan = NULL;
	foreach_ptr(subPlan, subPlanList) {
		PlannedStmt *plannedStmt = subPlan->plan;
		uint32 subPlanId = subPlan->subPlanId;
		ParamListInfo params = NULL;
		char *resultId = GenerateResultId(planId, subPlanId);
		bool useIntermediateResult = false;
		IntermediateResultsHashEntry *entry =
			SearchIntermediateResult(intermediateResultsHash, resultId);
			
    
		if (plannedStmt != NULL && plannedStmt->planTree != NULL plannedStmt->commandType == CMD_SELECT && plannedStmt->hasReturning == false 
			&& plannedStmt->hasModifyingCTE == false && plannedStmt->subplans == NULL && plannedStmt->rtable != NULL && list_length(entry->nodeIdList) == 0 
			&& entry->writeLocalFile == true){
			CustomScan *customScan = (CustomScan *)plannedStmt->planTree;
			if (list_length(customScan->custom_private) == 1 && list_length(task->taskPlacementList) == 1 
				&&CitusIsA((Node *) linitial(customScan->custom_private), DistributedPlan)) {
				DistributedPlan *node1 = GetDistributedPlan(customScan);
 			 	Task *task = (Task *)linitial(node1->workerJob->taskList);
 			 	RangeTblEntry *rte = NULL;
				foreach_ptr(rte, plannedStmt->rtable) {
					if (rte->eref != NULL && strcmp(rte->eref->aliasname, "intermediate_result") == 0 && rte->rtekind == RTE_FUNCTION) {
						useIntermediateResult = true;
						break;
					}
				}
				if (!useIntermediateResult && task->taskQuery.data.queryStringLazy != NULL) {
					SubPlanParallel plan = (SubPlanParallel) palloc0(sizeof(SubPlanParallel));
					plan->subPlan = subPlan;
					plan->fileName = QueryResultFileName(resultId);
					plan->fc = FileCompatFromFileStart(FileOpenForTransmit(plan->fileName,
																			 fileFlags,
																			 fileMode));
					ShardPlacement* en = (ShardPlacement*) linitial(task->taskPlacementList);
					plan->nodeName = en->nodeName;
					plan->nodePort = en->nodePort; 
					plan->queryStringLazy = task->taskQuery.data.queryStringLazy;
					parallelJobList = lappend(parallelJobList, plan);
				} else {
					sequenceJobList = lappend(sequenceJobList, subPlan);
				}
			} else {
				sequenceJobList = lappend(sequenceJobList, subPlan);
			}
			
		} else {
			sequenceJobList = lappend(sequenceJobList, subPlan);
		}
	}
	// 2. run these independent subplans parallel
	//WaitEvent *events = NULL;
	
	SubPlanParallel* pSubPlan = NULL;
	foreach_ptr(pSubPlan, parallelJobList) {
		char conninfo[100];
		sprintf(conninfo, "host=%s dbname=postgres user=postgres password=password port=%d", pSubPlan->nodeName, pSubPlan->nodePort);
		ereport(DEBUG3, (errmsg("conninfo:%s",conninfo)));
		pSubPlan->conn = PQconnectStart(conninfo);
		ConnStatusType  ConnType = PQstatus(pSubPlan->conn);
		ereport(DEBUG3, (errmsg("ConnStatusType:%d",ConnType)));
		if (CONNECTION_BAD == ConnType) {
			ereport(DEBUG3, (errmsg("bad ConnStatusType:%d",ConnType)));
			return;
		}
		PostgresPollingStatusType polltype = PGRES_POLLING_FAILED;
		while (true)
		{
			polltype = PQconnectPoll(pSubPlan->conn);
			if (polltype == PGRES_POLLING_FAILED) {
				ereport(DEBUG3, (errmsg("bad PostgresPollingStatusType:%d",polltype)));
				return;
			}
			if (polltype == PGRES_POLLING_OK)
				break;
		}
		int rc1 = PQsendQueryParams(pSubPlan->conn, pSubPlan->queryStringLazy, 0, NULL,
							   NULL, NULL, NULL, 1);
	}
	ereport(DEBUG3, (errmsg("create connection and send query success")));
	CopyOutState copyOutState1 = (CopyOutState) palloc0(sizeof(CopyOutStateData));
	copyOutState1->delim = (char *) delimiterCharacter;
	copyOutState1->null_print = (char *) nullPrintCharacter;
	copyOutState1->null_print_client = (char *) nullPrintCharacter;
	//copyOutState1->binary = CanUseBinaryCopyFormat(inputTupleDescriptor);
	copyOutState1->binary = true;
	copyOutState1->fe_msgbuf = makeStringInfo();
	copyOutState1->need_transcoding = false;
	foreach_ptr(pSubPlan, parallelJobList) {
		PGresult   *res1;
		res1 = PQgetResult(pSubPlan->conn);
		int nFields = PQnfields(res1);
		int columnCount = nFields;
		ereport(DEBUG3, (errmsg("nFields:%d, columnCount:%d",nFields,columnCount)));
		for (int i = 0; i < nFields; i++) {
			ereport(DEBUG3, (errmsg("%d, %-15s, oid:%d",i ,PQfname(res1, i),PQftype(res1,i))));
		}
		FmgrInfo *fi = NULL;
		//CopyCoercionData *ccd = NULL;
		Oid *typeArray = NULL;
		int availableColumnCount = 0;
		CopyOutState copyOutState = copyOutState1;
		resetStringInfo(copyOutState->fe_msgbuf);
		/* Signature */
		CopySendData(copyOutState, BinarySignature, 11);
	
		/* Flags field (no OIDs) */
		CopySendInt32(copyOutState, zero);
	
		/* No header extension */
		CopySendInt32(copyOutState, zero);
		WriteToLocalFile(copyOutState->fe_msgbuf, &fc1);
		while (true)
		{
			ereport(DEBUG3, (errmsg("+++++++++nFields:%d, columnCount:%d",nFields,columnCount)));
			ereport(DEBUG3, (errmsg("walk into while (true) 1")));
			if (!res1)
				break;
			if (fi == NULL) {
				typeArray = palloc0(nFields * sizeof(Oid));
				int columnIndex = 0;
				for (; columnIndex < columnCount; columnIndex++)
				{
					ereport(DEBUG3, (errmsg("columnIndex:%d",columnIndex)));
					typeArray[columnIndex] = PQftype(res1,columnIndex);
					ereport(DEBUG3, (errmsg("typeArray[columnIndex]:%d",typeArray[columnIndex])));
					if (typeArray[columnIndex] != InvalidOid) {
						ereport(DEBUG3, (errmsg("typeArray[columnIndex] != InvalidOid")));
						availableColumnCount++;
					}
					ereport(DEBUG3, (errmsg("PQftype: columnIndex:%d,  typid:%d",columnIndex, PQftype(res1,columnIndex))));
				}
				ereport(DEBUG3, (errmsg("11111")));
				fi = TypeOutputFunctions(columnCount, typeArray, true);
				ereport(DEBUG3, (errmsg("22222")));
			}
			ereport(DEBUG3, (errmsg("3333")));
			// if (ccd == NULL) {
			// 	ccd = palloc0(columnCount * sizeof(CopyCoercionData));
			// 	for (int columnIndex = 0; columnIndex < columnCount; columnIndex++)
			// 	{
			// 		Oid inputTupleType = typeArray[columnIndex];
			// 		char *columnName = PQfname(res1,columnIndex);
	
			// 		if (inputTupleType == InvalidOid)
			// 		{
			// 			/* TypeArrayFromTupleDescriptor decided to skip this column */
			// 			continue;
			// 		}
			// 	}
			// }
			// write to local file
			ereport(DEBUG3, (errmsg("PQntuples:%d",PQntuples(res1))));
			for (int i = 0; i < PQntuples(res1); i++)
			{
				Datum *columnValues = palloc0(nFields * sizeof(Datum));
				bool *columnNulls = palloc0(nFields * sizeof(bool));
				int *columeSizes = palloc0(nFields * sizeof(int));
				memset(columnValues, 0, nFields * sizeof(Datum));
				memset(columnNulls, 0, nFields * sizeof(bool));
				memset(columeSizes, 0, nFields * sizeof(int));
				for (int j = 0; j < nFields; j++){
					//ereport(DEBUG3, (errmsg("%-15s",PQgetvalue(res1, i, j))));
					if (PQgetisnull(res1, i, j))
					{
						columnValues[j] = NULL;
						columnNulls[j] = true;
					}else {
						char *value = PQgetvalue(res1, i, j);
						if (copyOutState->binary){
							if (PQfformat(res1, j) == 0){
								ereport(ERROR, (errmsg("unexpected text result")));
							}
						}
						columnValues[j] = (Datum)value;
					}
					columeSizes[j] = PQgetlength(res1,i,j);
					//char *value = PQgetvalue(res1, i, j);
					//columnValues[j] = (Datum)value;
					//AppendCopyRowData
					// RemoteFileDestReceiver *resultDest = (RemoteFileDestReceiver *) copyDest1;
					// CopyOutState copyOutState = resultDest->copyOutState;
				}
				ereport(DEBUG3, (errmsg("44444")));
				uint32 appendedColumnCount = 0;
				resetStringInfo(copyOutState->fe_msgbuf);
				bool binary = true;
				if (copyOutState->binary)
				{
					ereport(DEBUG3, (errmsg("CopySendInt16")));
					CopySendInt16(copyOutState, nFields);
				}
				for (uint32 columnIndex = 0; columnIndex < columnCount; columnIndex++){
					//ereport(DEBUG3, (errmsg("44444------")));
					Datum value = columnValues[columnIndex];
					int size = columeSizes[columnIndex];
					//ereport(DEBUG3, (errmsg("44444@@@@@")));
					//ereport(DEBUG3, (errmsg("44444@@@@@,%s",(char *)value)));
					bool isNull = columnNulls[columnIndex];
					bool lastColumn = false;
					if (typeArray[columnIndex] == InvalidOid) {
						continue;
					} else if (binary) {
						if (!isNull) {
							CopySendInt32(copyOutState, size);
							CopySendData(copyOutState, (char *)value, size);
						}
						else
						{
							//ereport(DEBUG3, (errmsg("4.4")));
							CopySendInt32(copyOutState, -1);
						}
					} else {
						if (!isNull) {
							FmgrInfo *outputFunctionPointer = &fi[columnIndex];
							CopyAttributeOutText(copyOutState, (char *)value);
						} else {
							CopySendString(copyOutState, copyOutState->null_print_client);
						}
						lastColumn = ((appendedColumnCount + 1) == availableColumnCount);
						if (!lastColumn){
							CopySendChar(copyOutState, copyOutState->delim[0]);
						}
	
					}
					appendedColumnCount++;
				}
				//ereport(DEBUG3, (errmsg("55555")));
				if (!copyOutState->binary)
				{
					/* append default line termination string depending on the platform */
			#ifndef WIN32
					CopySendChar(copyOutState, '\n');
			#else
					CopySendString(copyOutState, "\r\n");
			#endif
				}
				//ereport(DEBUG3, (errmsg("66666")));
				WriteToLocalFile(copyOutState->fe_msgbuf, &fc1);	
				//ereport(DEBUG3, (errmsg("WriteToLocalFile success, data :%s"),copyOutState->fe_msgbuf->data));	
				//ereport(DEBUG3, (errmsg("77777")));
			}
			res1 = PQgetResult(pSubPlan->conn);
		}
		/*res = PQexec(conn, "END");*/
		PQclear(res1);
		/* close the connection to the database and cleanup */
		PQfinish(pSubPlan->conn);
		ereport(DEBUG3, (errmsg("PQfinish(conn1);")));
	}

	// 3. run dependent subplans sequentially

	// List *newDistrubutedSubPlans = NULL;
	// DistributedSubPlan *subPlanxx = NULL;
	// int i = 0;
	// DistributedSubPlan *subPlan1 = NULL;
	// DistributedSubPlan *subPlan2 = NULL;
	// foreach_ptr(subPlanxx, subPlanList)
	// {	
	// 	if (i== 0 || i==1) {
	// 		newDistrubutedSubPlans = lappend(newDistrubutedSubPlans, subPlanxx);
	// 	}
	// 	if (i==0) {
	// 		subPlan1 = subPlanxx;
	// 	} else if (i==1) {
	// 		subPlan2 = subPlanxx;
	// 	}
	// 	i++;
	// }
	// char *conninfo1 = "host=172.31.87.38 dbname=postgres user=postgres password=password port=60003";// connect_timeout = 5";
	// char *conninfo2 = "host=172.31.87.38 dbname=postgres user=postgres password=password port=60002";// connect_timeout = 5";
	// PGconn *conn1 = PQconnectStart(conninfo1);
	// ConnStatusType  ConnType = PQstatus(conn1);
	// ereport(DEBUG3, (errmsg("ConnStatusType:%d",ConnType)));
	// if (CONNECTION_BAD == ConnType) {
	// 	ereport(DEBUG3, (errmsg("bad ConnStatusType:%d",ConnType)));
	// }

	// PGconn *conn2 = PQconnectStart(conninfo2);
	// ConnStatusType  ConnType2 = PQstatus(conn2);
	// ereport(DEBUG3, (errmsg("ConnStatusType:%d",ConnType2)));
	// if (CONNECTION_BAD == ConnType2) {
	// 	ereport(DEBUG3, (errmsg("bad ConnStatusType:%d",ConnType2)));
	// }
	// PostgresPollingStatusType polltype = PGRES_POLLING_FAILED;
	// while (true)
	// {
	// 	polltype = PQconnectPoll(conn1);
	// 	if (polltype == PGRES_POLLING_FAILED)
	// 		ereport(DEBUG3, (errmsg("bad PostgresPollingStatusType:%d",polltype)));
	// 	if (polltype == PGRES_POLLING_OK)
	// 		break;
	// }
	// while (true)
	// {
	// 	polltype = PQconnectPoll(conn2);
	// 	if (polltype == PGRES_POLLING_FAILED)
	// 		ereport(DEBUG3, (errmsg("bad PostgresPollingStatusType:%d",polltype)));
	// 	if (polltype == PGRES_POLLING_OK)
	// 		break;
	// }
	// ereport(DEBUG3, (errmsg("create connection success")));
	// // MultiConnection *connection = StartNodeUserDatabaseConnection(0,
	// // 																  "172.31.87.38",
	// // 																  60003,
	// // 																  NULL, NULL);
	// // WorkerSession *session = (WorkerSession *) palloc0(sizeof(WorkerSession));
	// // session->sessionId = 1;
	// // session->connection = connection;
	// // session->commandsSent = 0;

	// // MultiConnection *connection2 = StartNodeUserDatabaseConnection(0,
	// // 																  "172.31.87.38",
	// // 																  60002,
	// // 																  NULL, NULL);

	// // WorkerSession *session2 = (WorkerSession *) palloc0(sizeof(WorkerSession));
	// // session2->sessionId = 2;
	// // session2->connection = connection2;
	// // session2->commandsSent = 0;
	// DistributedPlan *node1 = GetDistributedPlan((CustomScan *) subPlan1->plan->planTree);
	// DistributedPlan *node2 = GetDistributedPlan((CustomScan *) subPlan2->plan->planTree);
 //    Task *task1 = (Task *)linitial(node1->workerJob->taskList);
 //    Task *task2 = (Task *)linitial(node2->workerJob->taskList);
	// //int rc1 = PQsendQuery(conn1, task1->taskQuery.data.queryStringLazy);
	// int rc1 = PQsendQueryParams(conn1, task1->taskQuery.data.queryStringLazy, 0, NULL,
	// 						   NULL, NULL, NULL, 1);
	// //int rc2 = PQsendQuery(conn2, task2->taskQuery.data.queryStringLazy);
	// int rc2 = PQsendQueryParams(conn2, task2->taskQuery.data.queryStringLazy, 0, NULL,
	// 						   NULL, NULL, NULL, 1);
	// ereport(DEBUG3, (errmsg("rc1:%d, rc2:%d, sql:%s",rc1,rc2,task1->taskQuery.data.queryStringLazy)));
	// ereport(DEBUG3, (errmsg("rc1:%s, rc2:%s",conn1->errorMessage.data,conn2->errorMessage.data)));

	// const int fileFlags = (O_APPEND | O_CREAT | O_RDWR | O_TRUNC | PG_BINARY);
	// const int fileMode = (S_IRUSR | S_IWUSR);

	// /* make sure the directory exists */
	// CreateIntermediateResultsDirectory();
	// const char *fileName1 =  QueryResultFileName( GenerateResultId(planId, subPlan1->subPlanId));
	// const char *fileName2 =  QueryResultFileName( GenerateResultId(planId, subPlan2->subPlanId));
	// ereport(DEBUG3, (errmsg("fileName1:%s, fileName2:%s",fileName1,fileName2)));
	// FileCompat fc1 = FileCompatFromFileStart(FileOpenForTransmit(fileName1,
	// 																		 fileFlags,
	// 																		 fileMode));
	// FileCompat fc2 = FileCompatFromFileStart(FileOpenForTransmit(fileName2,
	// 																		 fileFlags,
	// 																		 fileMode));
	// // config DestReceiver
	// const char *delimiterCharacter = "\t";
	// const char *nullPrintCharacter = "\\N";
	// //EState *estate = CreateExecutorState();
	// // DestReceiver *copyDest1 =
	// // 		CreateRemoteFileDestReceiver(GenerateResultId(planId, subPlan1->subPlanId), estate, NULL,
	// // 									 true);	
	// CopyOutState copyOutState1 = (CopyOutState) palloc0(sizeof(CopyOutStateData));
	// copyOutState1->delim = (char *) delimiterCharacter;
	// copyOutState1->null_print = (char *) nullPrintCharacter;
	// copyOutState1->null_print_client = (char *) nullPrintCharacter;
	// //copyOutState1->binary = CanUseBinaryCopyFormat(inputTupleDescriptor);
	// copyOutState1->binary = true;
	// copyOutState1->fe_msgbuf = makeStringInfo();
	// copyOutState1->need_transcoding = false;
	// //copyOutState->rowcontext = GetPerTupleMemoryContext(resultDest->executorState);
	// //RemoteFileDestReceiver *resultDest1 = (RemoteFileDestReceiver *) copyDest1;
	// //resultDest1->copyOutState = copyOutState;

	// // EState *estate2 = CreateExecutorState();
	// // DestReceiver *copyDest2 =
	// // 		CreateRemoteFileDestReceiver(GenerateResultId(planId, subPlan2->subPlanId), estate2, NULL,
	// // 									 true);		
	// CopyOutState copyOutState2 = (CopyOutState) palloc0(sizeof(CopyOutStateData));
	// copyOutState2->delim = (char *) delimiterCharacter;
	// copyOutState2->null_print = (char *) nullPrintCharacter;
	// copyOutState2->null_print_client = (char *) nullPrintCharacter;
	// //copyOutState2->binary = CanUseBinaryCopyFormat(inputTupleDescriptor);
	// copyOutState2->binary = true;
	// copyOutState2->fe_msgbuf = makeStringInfo();
	// copyOutState2->need_transcoding = false;
	// //copyOutState2->rowcontext = GetPerTupleMemoryContext(resultDest->executorState);
	// // RemoteFileDestReceiver *resultDest2 = (RemoteFileDestReceiver *) copyDest2;
	// // resultDest1->copyOutState = copyOutState2;


	// // get return value
	// PGresult   *res1;
	// PGresult   *res2;
	// res1 = PQgetResult(conn1);
	// int nFields = PQnfields(res1);
	// int columnCount = nFields;
	// ereport(DEBUG3, (errmsg("nFields:%d, columnCount:%d",nFields,columnCount)));
	// for (int i = 0; i < nFields; i++) {
	// 	ereport(DEBUG3, (errmsg("%d, %-15s, oid:%d",i ,PQfname(res1, i),PQftype(res1,i))));
	// }
	// FmgrInfo *fi = NULL;
	// //CopyCoercionData *ccd = NULL;
	// Oid *typeArray = NULL;
	// int availableColumnCount = 0;
	// CopyOutState copyOutState = copyOutState1;
	// resetStringInfo(copyOutState->fe_msgbuf);
	// /* Signature */
	// CopySendData(copyOutState, BinarySignature, 11);

	// /* Flags field (no OIDs) */
	// CopySendInt32(copyOutState, zero);

	// /* No header extension */
	// CopySendInt32(copyOutState, zero);
	// WriteToLocalFile(copyOutState->fe_msgbuf, &fc1);
	// while (true)
	// {
	// 	ereport(DEBUG3, (errmsg("+++++++++nFields:%d, columnCount:%d",nFields,columnCount)));
	// 	ereport(DEBUG3, (errmsg("walk into while (true) 1")));
	// 	if (!res1)
	// 		break;
	// 	if (fi == NULL) {
	// 		typeArray = palloc0(nFields * sizeof(Oid));
	// 		int columnIndex = 0;
	// 		for (; columnIndex < columnCount; columnIndex++)
	// 		{
	// 			ereport(DEBUG3, (errmsg("columnIndex:%d",columnIndex)));
	// 			typeArray[columnIndex] = PQftype(res1,columnIndex);
	// 			ereport(DEBUG3, (errmsg("typeArray[columnIndex]:%d",typeArray[columnIndex])));
	// 			if (typeArray[columnIndex] != InvalidOid) {
	// 				ereport(DEBUG3, (errmsg("typeArray[columnIndex] != InvalidOid")));
	// 				availableColumnCount++;
	// 			}
	// 			ereport(DEBUG3, (errmsg("PQftype: columnIndex:%d,  typid:%d",columnIndex, PQftype(res1,columnIndex))));
	// 		}
	// 		ereport(DEBUG3, (errmsg("11111")));
	// 		fi = TypeOutputFunctions(columnCount, typeArray, true);
	// 		ereport(DEBUG3, (errmsg("22222")));
	// 	}
	// 	ereport(DEBUG3, (errmsg("3333")));
	// 	// if (ccd == NULL) {
	// 	// 	ccd = palloc0(columnCount * sizeof(CopyCoercionData));
	// 	// 	for (int columnIndex = 0; columnIndex < columnCount; columnIndex++)
	// 	// 	{
	// 	// 		Oid inputTupleType = typeArray[columnIndex];
	// 	// 		char *columnName = PQfname(res1,columnIndex);

	// 	// 		if (inputTupleType == InvalidOid)
	// 	// 		{
	// 	// 			/* TypeArrayFromTupleDescriptor decided to skip this column */
	// 	// 			continue;
	// 	// 		}
	// 	// 	}
	// 	// }
	// 	// write to local file
	// 	ereport(DEBUG3, (errmsg("PQntuples:%d",PQntuples(res1))));
	// 	for (int i = 0; i < PQntuples(res1); i++)
	// 	{
	// 		Datum *columnValues = palloc0(nFields * sizeof(Datum));
	// 		bool *columnNulls = palloc0(nFields * sizeof(bool));
	// 		int *columeSizes = palloc0(nFields * sizeof(int));
	// 		memset(columnValues, 0, nFields * sizeof(Datum));
	// 		memset(columnNulls, 0, nFields * sizeof(bool));
	// 		memset(columeSizes, 0, nFields * sizeof(int));
	// 		for (int j = 0; j < nFields; j++){
	// 			//ereport(DEBUG3, (errmsg("%-15s",PQgetvalue(res1, i, j))));
	// 			if (PQgetisnull(res1, i, j))
	// 			{
	// 				columnValues[j] = NULL;
	// 				columnNulls[j] = true;
	// 			}else {
	// 				char *value = PQgetvalue(res1, i, j);
	// 				if (copyOutState->binary){
	// 					if (PQfformat(res1, j) == 0){
	// 						ereport(ERROR, (errmsg("unexpected text result")));
	// 					}
	// 				}
	// 				columnValues[j] = (Datum)value;
	// 			}
	// 			columeSizes[j] = PQgetlength(res1,i,j);
	// 			//char *value = PQgetvalue(res1, i, j);
	// 			//columnValues[j] = (Datum)value;
	// 			//AppendCopyRowData
	// 			// RemoteFileDestReceiver *resultDest = (RemoteFileDestReceiver *) copyDest1;
	// 			// CopyOutState copyOutState = resultDest->copyOutState;
	// 		}
	// 		ereport(DEBUG3, (errmsg("44444")));
	// 		uint32 appendedColumnCount = 0;
	// 		resetStringInfo(copyOutState->fe_msgbuf);
	// 		bool binary = true;
	// 		if (copyOutState->binary)
	// 		{
	// 			ereport(DEBUG3, (errmsg("CopySendInt16")));
	// 			CopySendInt16(copyOutState, nFields);
	// 		}
	// 		for (uint32 columnIndex = 0; columnIndex < columnCount; columnIndex++){
	// 			//ereport(DEBUG3, (errmsg("44444------")));
	// 			Datum value = columnValues[columnIndex];
	// 			int size = columeSizes[columnIndex];
	// 			//ereport(DEBUG3, (errmsg("44444@@@@@")));
	// 			//ereport(DEBUG3, (errmsg("44444@@@@@,%s",(char *)value)));
	// 			bool isNull = columnNulls[columnIndex];
	// 			bool lastColumn = false;
	// 			if (typeArray[columnIndex] == InvalidOid) {
	// 				continue;
	// 			} else if (binary) {
	// 				if (!isNull) {
	// 					//ereport(DEBUG3, (errmsg("4.1")));
	// 					//FmgrInfo *outputFunctionPointer = &fi[columnIndex];
	// 					//bytea *outputBytes = SendFunctionCall(outputFunctionPointer, value);
	// 					//bytea *outputBytes = DatumGetByteaP(value);
	// 					//ereport(DEBUG3, (errmsg("4.2")));
	// 					//CopySendInt32(copyOutState, VARSIZE(outputBytes) - VARHDRSZ);
	// 					CopySendInt32(copyOutState, size);
	// 					//ereport(DEBUG3, (errmsg("4.3")));
	// 					//CopySendData(copyOutState, VARDATA(outputBytes),
	// 					//	 VARSIZE(outputBytes) - VARHDRSZ);
	// 					CopySendData(copyOutState, (char *)value, size);
	// 				}
	// 				else
	// 				{
	// 					//ereport(DEBUG3, (errmsg("4.4")));
	// 					CopySendInt32(copyOutState, -1);
	// 				}
	// 			} else {
	// 				if (!isNull) {
	// 					FmgrInfo *outputFunctionPointer = &fi[columnIndex];
	// 					//ereport(DEBUG3, (errmsg("44444+,%d",outputFunctionPointer->fn_oid)));
	// 					//char *columnText = OutputFunctionCall(outputFunctionPointer, value);
	// 					//ereport(DEBUG3, (errmsg("44444++++,%s",columnText)));
	// 					//CopyAttributeOutText(copyOutState, columnText);
	// 					CopyAttributeOutText(copyOutState, (char *)value);
	// 				} else {
	// 					CopySendString(copyOutState, copyOutState->null_print_client);
	// 				}
	// 				lastColumn = ((appendedColumnCount + 1) == availableColumnCount);
	// 				if (!lastColumn){
	// 					CopySendChar(copyOutState, copyOutState->delim[0]);
	// 				}

	// 			}
	// 			appendedColumnCount++;
	// 		}
	// 		//ereport(DEBUG3, (errmsg("55555")));
	// 		if (!copyOutState->binary)
	// 		{
	// 			/* append default line termination string depending on the platform */
	// 	#ifndef WIN32
	// 			CopySendChar(copyOutState, '\n');
	// 	#else
	// 			CopySendString(copyOutState, "\r\n");
	// 	#endif
	// 		}
	// 		//ereport(DEBUG3, (errmsg("66666")));
	// 		WriteToLocalFile(copyOutState->fe_msgbuf, &fc1);	
	// 		//ereport(DEBUG3, (errmsg("WriteToLocalFile success, data :%s"),copyOutState->fe_msgbuf->data));	
	// 		//ereport(DEBUG3, (errmsg("77777")));
	// 	}
	// 	res1 = PQgetResult(conn1);
	// }
	// /*res = PQexec(conn, "END");*/
	// PQclear(res1);
	// /* close the connection to the database and cleanup */
	// PQfinish(conn1);
	// ereport(DEBUG3, (errmsg("PQfinish(conn1);")));
	// //sleep(60);	
	// res2 = PQgetResult(conn2);
	// nFields = PQnfields(res2);
	// for (int i = 0; i < nFields; i++) {
	// 	ereport(DEBUG3, (errmsg("%-15s",PQfname(res2, i))));
	// }
	// fi = NULL;
	// availableColumnCount = 0;
	// resetStringInfo(copyOutState->fe_msgbuf);
	// /* Signature */
	// CopySendData(copyOutState, BinarySignature, 11);

	// /* Flags field (no OIDs) */
	// CopySendInt32(copyOutState, zero);

	// /* No header extension */
	// CopySendInt32(copyOutState, zero);
	// WriteToLocalFile(copyOutState->fe_msgbuf, &fc2);
	// while (true)
	// {
	// 	if (!res2)
	// 		break;
	// 	if (fi == NULL) {
	// 		typeArray = palloc0(nFields * sizeof(Oid));
	// 		int columnIndex = 0;
	// 		for (; columnIndex < columnCount; columnIndex++)
	// 		{
	// 			//ereport(DEBUG3, (errmsg("columnIndex:%d",columnIndex)));
	// 			typeArray[columnIndex] = PQftype(res2,columnIndex);
	// 			//ereport(DEBUG3, (errmsg("typeArray[columnIndex]:%d",typeArray[columnIndex])));
	// 			if (typeArray[columnIndex] != InvalidOid) {
	// 				//ereport(DEBUG3, (errmsg("typeArray[columnIndex] != InvalidOid")));
	// 				availableColumnCount++;
	// 			}
	// 			//ereport(DEBUG3, (errmsg("PQftype: columnIndex:%d,  typid:%d",columnIndex, PQftype(res1,columnIndex))));
	// 		}
	// 		//ereport(DEBUG3, (errmsg("11111")));
	// 		fi = TypeOutputFunctions(columnCount, typeArray, true);
	// 		//ereport(DEBUG3, (errmsg("22222")));
	// 	}
	// 	CopyOutState copyOutState = copyOutState1;
	// 	for (int i = 0; i < PQntuples(res2); i++)
	// 	{
	// 		Datum *columnValues = palloc0(nFields * sizeof(Datum));
	// 		bool *columnNulls = palloc0(nFields * sizeof(bool));
	// 		int *columeSizes = palloc0(nFields * sizeof(int));
	// 		memset(columnValues, 0, nFields * sizeof(Datum));
	// 		memset(columnNulls, 0, nFields * sizeof(bool));
	// 		memset(columeSizes, 0, nFields * sizeof(int));

	// 		for (int j = 0; j < nFields; j++){
	// 			//ereport(DEBUG3, (errmsg("%-15s",PQgetvalue(res1, i, j))));
	// 			if (PQgetisnull(res2, i, j))
	// 			{
	// 				columnValues[j] = NULL;
	// 				columnNulls[j] = true;
	// 			} else {
	// 				char *value = PQgetvalue(res2, i, j);
	// 				if (copyOutState->binary){
	// 					if (PQfformat(res2, j) == 0){
	// 						ereport(ERROR, (errmsg("unexpected text result")));
	// 					}
	// 				}
	// 				columnValues[j] = (Datum)value;
	// 			}
	// 			columeSizes[j] = PQgetlength(res2,i,j);
	// 			//AppendCopyRowData
	// 			// RemoteFileDestReceiver *resultDest = (RemoteFileDestReceiver *) copyDest1;
	// 			// CopyOutState copyOutState = resultDest->copyOutState;
	// 		}
	// 		//ereport(DEBUG3, (errmsg("44444")));
	// 		uint32 appendedColumnCount = 0;
	// 		resetStringInfo(copyOutState->fe_msgbuf);
	// 		bool binary = true;
	// 		if (copyOutState->binary)
	// 		{
	// 			ereport(DEBUG3, (errmsg("CopySendInt16")));
	// 			CopySendInt16(copyOutState, nFields);
	// 		}
	// 		for (uint32 columnIndex = 0; columnIndex < columnCount; columnIndex++){
	// 			//ereport(DEBUG3, (errmsg("44444------")));
	// 			Datum value = columnValues[columnIndex];
	// 			int size = columeSizes[columnIndex];
	// 			//ereport(DEBUG3, (errmsg("44444@@@@@,%s",(char *)value)));
	// 			bool isNull = columnNulls[columnIndex];
	// 			bool lastColumn = false;
	// 			if (typeArray[columnIndex] == InvalidOid) {
	// 				continue;
	// 			} else if (binary) {
	// 				if (!isNull) {
	// 					//FmgrInfo *outputFunctionPointer = &fi[columnIndex];
	// 					//bytea *outputBytes = SendFunctionCall(outputFunctionPointer, value);
	// 					//bytea *outputBytes = DatumGetByteaP(value);
	// 					//ereport(DEBUG3, (errmsg("4.1")));
	// 					//bytea *outputBytes = DatumGetByteaP(value);
	// 					//ereport(DEBUG3, (errmsg("4.2")));
	// 					//CopySendInt32(copyOutState, VARSIZE(outputBytes) - VARHDRSZ);
	// 					CopySendInt32(copyOutState, size);
	// 					//ereport(DEBUG3, (errmsg("4.3")));
	// 					//CopySendData(copyOutState, VARDATA(outputBytes),
	// 					//	 VARSIZE(outputBytes) - VARHDRSZ);
	// 					CopySendData(copyOutState, (char *)value, size);
	// 				}
	// 				else
	// 				{
	// 					CopySendInt32(copyOutState, -1);
	// 				}
	// 			} else {
	// 				if (!isNull) {
	// 					FmgrInfo *outputFunctionPointer = &fi[columnIndex];
	// 					//ereport(DEBUG3, (errmsg("44444+,%d",outputFunctionPointer->fn_oid)));
	// 					//char *columnText = OutputFunctionCall(outputFunctionPointer, value);
	// 					//ereport(DEBUG3, (errmsg("44444++++,%s",columnText)));
	// 					//CopyAttributeOutText(copyOutState, columnText);
	// 					CopyAttributeOutText(copyOutState, (char *)value);
	// 				} else {
	// 					CopySendString(copyOutState, copyOutState->null_print_client);
	// 				}
	// 				lastColumn = ((appendedColumnCount + 1) == availableColumnCount);
	// 				if (!lastColumn){
	// 					CopySendChar(copyOutState, copyOutState->delim[0]);
	// 				}

	// 			}
	// 			appendedColumnCount++;
	// 		}
	// 		//ereport(DEBUG3, (errmsg("55555")));
	// 		if (!copyOutState->binary)
	// 		{
	// 			/* append default line termination string depending on the platform */
	// 	#ifndef WIN32
	// 			CopySendChar(copyOutState, '\n');
	// 	#else
	// 			CopySendString(copyOutState, "\r\n");
	// 	#endif
	// 		}
	// 		//ereport(DEBUG3, (errmsg("66666")));
	// 		WriteToLocalFile(copyOutState->fe_msgbuf, &fc2);	
	// 		//ereport(DEBUG3, (errmsg("WriteToLocalFile success, data :%s"),copyOutState->fe_msgbuf->data));	
	// 		//ereport(DEBUG3, (errmsg("77777")));
	// 	}
	// 	res2 = PQgetResult(conn2);
	// }
	// /*res = PQexec(conn, "END");*/
	// PQclear(res2);
	// /* close the connection to the database and cleanup */
	// PQfinish(conn2);


	/* ------------- danny test end ---------------  */

	//DistributedSubPlan *subPlan = NULL;
	//i =0 ;
	foreach_ptr(subPlan, sequenceJobList)
	//foreach_ptr(subPlan, subPlanList)
	{
		/* ------------- danny test begin ---------------  */
		// if (i== 0 || i==1) {
		// 	i++;
		// 	continue;
		// }
		// if (i==5){
		// 	sleep(200);
		// }
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
		// WorkerNode *workerNode = NULL;
		// foreach_ptr(workerNode, remoteWorkerNodeList)
		// {
		// 	ereport(DEBUG3, (errmsg("$$$$$$$$$$$$$$$$$$Subplan %s will be sent to %s:%d" ,resultId,workerNode->workerName, workerNode->workerPort)));
		// }
		IntermediateResultsHashEntry *entry =
			SearchIntermediateResult(intermediateResultsHash, resultId);
		ereport(DEBUG3, (errmsg("$$$$$$$$$$$$$$$$$$ entry node_id_length:%d,writeLocalFile:%d ", list_length(entry->nodeIdList),entry->writeLocalFile)));
		elog_node_display(LOG, "$$$$$$$$$$$$$$$$$$ entry node_id_list: parse tree", entry->nodeIdList, Debug_pretty_print);
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
		i++;
	}
}
