package com.github.lechandonga.contentaddressedobjectstorejava.store.api;

/**
 * Summary of one garbage-collection pass.
 *
 * @param blobsScanned   blobs present in the content area before the pass
 * @param blobsReachable blobs referenced by at least one committed version
 * @param blobsReclaimed unreferenced blobs deleted during the pass
 * @param tempFilesRemoved stale staging files swept from the temp area
 * @param orphanMetadataRecovered blobs rescued from the quarantine area
 */
public record GarbageCollectionReport(long blobsScanned, long blobsReachable, long blobsReclaimed,
                                      long tempFilesRemoved, long orphanMetadataRecovered) {
}
