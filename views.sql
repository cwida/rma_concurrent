/**
 * General information about the parameters of a single execution
 */

CREATE VIEW view_executions AS
SELECT
	e.id AS exec_id,
	CASE(e.algorithm)
		WHEN 'btree_v2' THEN 'abtree'
		ELSE e.algorithm
	END AS algorithm,
	CAST(p1.value AS INT) AS index_b,
	CAST(p2.value AS INT) AS leaf_b,
	e.experiment,
	CASE WHEN pi0.exec_id IS NULL THEN 0 ELSE CAST(pi0.value AS INT) END AS initial_size,
	CAST(pi1.value AS INT) AS num_insertions,
	CAST(pi2.value AS INT) AS num_lookups,
	CASE WHEN pi3.value IS NULL THEN 0 ELSE CAST(pi3.value AS INT) END AS num_scans,
	pd1.value AS distribution,
	CASE WHEN pd2.value IS NULL THEN 0.0 ELSE CAST(pd2.value AS real) END AS alpha,
	CASE WHEN pd3.value IS NULL THEN 0.0 ELSE CAST(pd3.value AS real) END AS beta,
    CASE WHEN pmem.value IS NULL THEN 0 ELSE CAST(pmem.value AS INT) END AS memory_pool,
	REPLACE(phost.value, '.scilens.private', '') AS hostname,
	CASE
	    WHEN SUBSTR(phost.value, 1, 8) == 'diamonds' THEN 'diamonds'
		WHEN SUBSTR(phost.value, 1, 6) == 'bricks' THEN 'bricks'
	    WHEN SUBSTR(phost.value, 1, 5) == 'rocks' THEN 'rocks'
	    ELSE 'unknown'
    END AS cluster,
	CASE WHEN prho0.value IS NULL THEN 0.0 ELSE CAST(prho0.value AS real) END AS rho_0,
	CASE WHEN prhoh.value IS NULL THEN 0.0 ELSE CAST(prhoh.value AS real) END AS rho_h,
	CASE WHEN pthetah.value IS NULL THEN 0.0 ELSE CAST(pthetah.value AS real) END AS theta_h,
	CASE WHEN ptheta0.value IS NULL THEN 0.0 ELSE CAST(ptheta0.value AS real) END AS theta_0,
	CASE WHEN pidlsGroupSize.exec_id IS NULL THEN 0 ELSE CAST(pidlsGroupSize.value AS int) END AS idls_group_size,
	CASE WHEN pApmaRank.exec_id IS NULL THEN 0.0 ELSE CAST(pApmaRank.value AS real) END AS apma_rank,
	CASE WHEN pApmaSampling.exec_id IS NULL THEN 0.0 ELSE CAST (pApmaSampling.value AS real) END AS apma_sampling,
	CASE WHEN pExtentSize.exec_id IS NULL THEN 0 ELSE CAST(pExtentSize.value AS int) END as extent_size,
	CASE
        WHEN pHugeTlb.value IS NULL THEN 0
		WHEN pHugeTlb.value = '1' OR pHugeTlb.value = 'true' THEN 1
        ELSE 0
	END AS hugetlb,
	CASE WHEN pUpdateThreads.value IS NULL THEN 0 ELSE CAST(pUpdateThreads.value AS INT) END AS parallelism_updates,
	CASE WHEN pScanThreads.value IS NULL THEN 0 ELSE CAST(pScanThreads.value AS INT) END AS parallelism_scans,
	CASE WHEN pDelay.value IS NULL THEN 0 ELSE CAST(pDelay.value AS INT) END AS delay_millisecs,
	timeStart AS timeStart,
	timeEnd AS timeEnd
FROM executions e
	JOIN parameters p1 ON (e.id = p1.exec_id AND p1.name = 'inode_block_size')
	JOIN parameters p2 ON (e.id = p2.exec_id AND p2.name = 'leaf_block_size')
	LEFT JOIN parameters pi0 ON (e.id = pi0.exec_id AND pi0.name = 'initial_size')
	JOIN parameters pi1 ON (e.id = pi1.exec_id AND pi1.name = 'num_insertions')
	JOIN parameters pi2 ON (e.id = pi2.exec_id AND pi2.name = 'num_lookups')
	LEFT JOIN parameters pi3 ON (e.id = pi3.exec_id AND pi3.name = 'num_scans')
	JOIN parameters pd1 ON (e.id = pd1.exec_id AND pd1.name = 'distribution')
	LEFT JOIN parameters pd2 ON(e.id = pd2.exec_id AND pd2.name = 'alpha')
	LEFT JOIN parameters pd3 ON(e.id = pd3.exec_id AND pd3.name = 'beta')
	LEFT JOIN parameters pmem ON (e.id = pmem.exec_id AND pmem.name = 'memory_pool')
	LEFT JOIN parameters phost ON (e.id = phost.exec_id AND phost.name = 'hostname')
	LEFT JOIN parameters prho0 ON (e.id = prho0.exec_id AND prho0.name = 'rho_0')
	LEFT JOIN parameters prhoh ON (e.id = prhoh.exec_id AND prhoh.name = 'rho_h')
	LEFT JOIN parameters pthetah ON (e.id = pthetah.exec_id AND pthetah.name = 'theta_h')
	LEFT JOIN parameters ptheta0 ON (e.id = ptheta0.exec_id AND ptheta0.name = 'theta_0')
	 /* APMA & IDLS parameters */
	LEFT JOIN parameters pidlsGroupSize ON (e.id = pidlsGroupSize.exec_id AND pidlsGroupSize.name = 'idls_group_size')
	 /* The rank threshold for the Ehanced Adaptive PMA, in (0, 1] */
	LEFT JOIN parameters pApmaRank ON (e.id = pApmaRank.exec_id AND pApmaRank.name = 'apma_rank')
	 /* The sample threshold, whether to forward an insert/delete to the predictor */
	LEFT JOIN parameters pApmaSampling ON (e.id = pApmaSampling.exec_id AND pApmaSampling.name = 'apma_sampling_rate')
	 /* The `Predictor Scale' only affects the algorithm `apma_hu' and regards the size of the queue in the predictor = scale * log2(C) */
	LEFT JOIN parameters pApmaHuPredictorScale ON (e.id = pApmaHuPredictorScale.exec_id AND pApmaHuPredictorScale.name = 'apma_predictor_scale')
	LEFT JOIN parameters pExtentSize ON (e.id = pExtentSize.exec_id AND pExtentSize.name = 'extent_size')
	LEFT JOIN parameters pHugeTlb ON (e.id = pHugeTlb.exec_id AND pHugeTlb.name = 'hugetlb')
	LEFT JOIN parameters pUpdateThreads ON (e.id = pUpdateThreads.exec_id AND pUpdateThreads.name = 'thread_inserts')
	LEFT JOIN parameters pScanThreads ON (e.id = pScanThreads.exec_id AND pScanThreads.name = 'thread_scans')
	LEFT JOIN parameters pDelay ON (e.id = pDelay.exec_id AND pDelay.name = 'delay')
;
  
/**
 * Information about the single executions of the experiment 'parallel_insert'
 * -- It depends on the view `view_executions'
 */
CREATE VIEW view_parallel_insert AS
SELECT
	e.exec_id,
	e.cluster,
	e.algorithm,
	e.index_b,
	e.leaf_b,
	e.num_insertions AS size,
	e.distribution,
	e.alpha,
	e.beta,
	e.extent_size,
	e.apma_rank,
	e.apma_sampling,
	e.parallelism_updates,
	e.parallelism_scans,
	(e.parallelism_updates + e.parallelism_scans) AS parallelism_degree,
	e.delay_millisecs,
	t.time_insert AS completion_time_microsecs,
	(CAST(e.num_insertions AS REAL) / t.time_insert) * 1000 * 1000 AS insert_throughput,
	(CAST(t.num_elements_scan AS REAL) / t.time_insert) * 1000 * 1000 AS scan_throughput
FROM parallel_insert t
JOIN view_executions e ON (t.exec_id = e.exec_id)
;

/**
 * Information about the single executions of the experiment 'parallel_idls'
 * -- It depends on the view `view_executions'
 */
CREATE VIEW view_parallel_idls AS
SELECT
	e.exec_id,
	e.cluster,
	e.algorithm,
	e.index_b,
	e.leaf_b,
	e.num_insertions AS size,
	e.distribution,
	e.alpha,
	e.beta,
	e.extent_size,
	e.apma_rank,
	e.apma_sampling,
	e.idls_group_size,
	e.parallelism_updates,
	e.parallelism_scans,
	(e.parallelism_updates + e.parallelism_scans) AS parallelism_degree,
	e.delay_millisecs,
	t.updates AS num_updates,
	t.t_updates_millisecs AS updates_completion_time_millisecs,
	(CAST(t.updates AS REAL) / t.t_updates_millisecs) * 1000 AS updates_throughput,
	t.scan_updates AS num_scan_elts,
	(CAST(t.scan_updates AS REAL) / t.t_updates_millisecs) * 1000 AS scan_throughput
FROM parallel_idls t
JOIN view_executions e ON (t.exec_id = e.exec_id)
;