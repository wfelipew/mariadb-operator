package controller

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/go-logr/logr"
	mariadbv1alpha1 "github.com/mariadb-operator/mariadb-operator/v25/api/v1alpha1"
	"github.com/mariadb-operator/mariadb-operator/v25/pkg/builder"
	condition "github.com/mariadb-operator/mariadb-operator/v25/pkg/condition"
	"github.com/mariadb-operator/mariadb-operator/v25/pkg/controller/replication"
	"github.com/mariadb-operator/mariadb-operator/v25/pkg/refresolver"
	"github.com/mariadb-operator/mariadb-operator/v25/pkg/sql"
	stsobj "github.com/mariadb-operator/mariadb-operator/v25/pkg/statefulset"
	"golang.org/x/mod/semver"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

func (r *MariaDBReconciler) reconcileExternalReplInit(ctx context.Context, mariadb *mariadbv1alpha1.MariaDB) (ctrl.Result, error) {
	logger := log.FromContext(ctx).WithName("external-repl-init")

	if !mariadb.IsReplicationEnabled() {
		logger.Info("replication is not enabled")
		return ctrl.Result{}, nil
	}
	replication := mariadb.Replication()

	if !replication.IsExternalReplication() {
		logger.Info("replication is enabled but is not external")
		return ctrl.Result{}, nil
	}

	if mariadb.HasConfiguredReplication() {
		logger.Info("replication is enabled and external but replication it is already configured")
		return ctrl.Result{}, nil
	}

	if mariadb.IsExternalReplInitialized() {
		logger.Info("external replication already initialized")
		return ctrl.Result{}, nil
	}

	logger.Info("reconciling external replication init")
	if err := r.patchStatus(ctx, mariadb, func(status *mariadbv1alpha1.MariaDBStatus) error {
		condition.SetExternalReplInitializing(status)
		return nil
	}); err != nil {
		return ctrl.Result{}, fmt.Errorf("error patching MariaDB status: %v", err)
	}

	logger.Info("handle init backup")
	if err := r.handleInitialBackup(ctx, mariadb, replication, logger); err != nil {
		return ctrl.Result{}, err
	}

	logger.Info("reconciling restore on each pod")
	total_pods := 0
	total_restored_pods := 0
	for _, i := range r.replicationPodIndexes(mariadb) {
		total_pods++
		if _, err := r.reconcileRestoreInPod(ctx, mariadb, i, logger, false); err == nil {
			total_restored_pods++
		}
	}

	if total_pods != total_restored_pods {
		logger.Info("restore in pod in-progress")
		return ctrl.Result{RequeueAfter: time.Minute * 1}, fmt.Errorf("restore in pod in-progress")
	}

	//cleanup the restore
	logger.Info("cleanning up the restore on each pod")
	for _, i := range r.replicationPodIndexes(mariadb) {
		_ = r.cleanupRestoreInPod(ctx, mariadb, i, logger)
	}

	logger.Info("reconciling restore finished")

	logger.Info("setting ExternalReplInitialized status to true")
	if err := r.patchStatus(ctx, mariadb, func(status *mariadbv1alpha1.MariaDBStatus) error {
		condition.SetExternalReplInitialized(status)
		return nil
	}); err != nil {
		return ctrl.Result{}, fmt.Errorf("error patching MariaDB status: %v", err)
	}

	logger.Info("reconciling external replication init finished")
	return ctrl.Result{}, nil
}

func (r *MariaDBReconciler) reconcileRestoreInPod(ctx context.Context, mariadb *mariadbv1alpha1.MariaDB,
	replicaPodIndex int, logger logr.Logger, removeCurrentPod bool) (ctrl.Result, error) {

	logger.Info("reconciling restore in pod", "pod", replicaPodIndex)

	// var client *sql.Client
	// var err error
	// if client, err = sql.NewClientWithMariaDB(ctx, mariadb, r.RefResolver); err != nil {
	// 	logger.Error(err, "error getting MariaDB client")
	// 	return ctrl.Result{}, fmt.Errorf("error getting MariaDB client: %v", err)
	// }

	replClientSet, err := replication.NewReplicationClientSet(mariadb, r.RefResolver)
	if err != nil {
		logger.Error(err, "error getting replica clientset", "err", err, "pod", replicaPodIndex)
		return ctrl.Result{RequeueAfter: 5 * time.Second}, err
	}

	client, err := replClientSet.ClientForIndex(ctx, replicaPodIndex)
	if err != nil {
		logger.Error(err, "error getting replica client", "err", err, "pod", replicaPodIndex)
		return ctrl.Result{RequeueAfter: 5 * time.Second}, err
	}
	defer client.Close()

	// req, err := r.NewReconcileRequest(ctx, mariadb)
	// client := req.replClientSet.clientForIndex(ctx, replicaPodIndex)

	if err := client.ResetMaster(ctx); err != nil {
		logger.Error(err, "error reseting master")
		return ctrl.Result{}, fmt.Errorf("error resetting master: %v", err)
	}

	var existingRestore mariadbv1alpha1.Restore
	err = r.Get(ctx, mariadb.RestoreKeyInPod(replicaPodIndex), &existingRestore)

	if err == nil && !existingRestore.IsComplete() {
		logger.Info("restore exists, but not complete", "pod", replicaPodIndex)
		return ctrl.Result{RequeueAfter: time.Second * 10}, fmt.Errorf("restore is not complete")
	}

	podKey := types.NamespacedName{
		Name:      stsobj.PodName(*mariadb.GetObjectMeta(), replicaPodIndex),
		Namespace: mariadb.Namespace,
	}

	if !existingRestore.IsComplete() {
		// Restore/Bootstrap node from backup
		logger.Info("restore does not exists, create a new restore", "pod", replicaPodIndex)

		if removeCurrentPod {
			logger.Info("Recreating Pod")
			// if err := r.ensurePodInitializing(ctx, podKey, logger); err != nil {
			// 	return ctrl.Result{}, fmt.Errorf("error ensuring Pod initializing: %v", err)
			// }

			pvcKey := mariadb.PVCKey(builder.StorageVolume, replicaPodIndex)
			var pvc corev1.PersistentVolumeClaim
			if err := r.Get(ctx, pvcKey, &pvc); err != nil {
				return ctrl.Result{}, fmt.Errorf("error getting pvc from Pod '%v': %v", replicaPodIndex, err)
			}
			if err := r.Delete(ctx, &pvc); err != nil {
				return ctrl.Result{}, fmt.Errorf("error deleting pvc from Pod '%v': %v", replicaPodIndex, err)
			}

			var existingPod corev1.Pod
			if err := r.Get(ctx, podKey, &existingPod); err != nil {
				return ctrl.Result{}, fmt.Errorf("error getting Pod '%v': %v", replicaPodIndex, err)
			}
			if err := r.Delete(ctx, &existingPod); err != nil {
				return ctrl.Result{}, fmt.Errorf("error deleting Pod '%v': %v", replicaPodIndex, err)
			}
		}
		err := newRestore(mariadb, *r, ctx, replicaPodIndex)
		return ctrl.Result{}, fmt.Errorf("new restore attempt%v", err)
	}

	logger.Info("restore complete", "pod", replicaPodIndex)
	// if err := r.Delete(ctx, &existingRestore); err != nil {
	// 	logger.Info("failed to delete restore", "pod", replicaPodIndex)
	// 	return ctrl.Result{}, fmt.Errorf("error deleting Restore: %v", err)
	// }

	logger.Info("restore finished", "pod", replicaPodIndex)
	return ctrl.Result{}, nil
}

func (r *MariaDBReconciler) cleanupRestoreInPod(ctx context.Context, mariadb *mariadbv1alpha1.MariaDB,
	replicaPodIndex int, logger logr.Logger) error {

	logger.Info("cleanning up restore in pod", "pod", replicaPodIndex)

	var existingRestore mariadbv1alpha1.Restore
	err := r.Get(ctx, mariadb.RestoreKeyInPod(replicaPodIndex), &existingRestore)

	if err == nil {
		if err := r.Delete(ctx, &existingRestore); err != nil {
			logger.Info("failed to delete restore", "pod", replicaPodIndex)
			return fmt.Errorf("error deleting Restore: %v", err)
		}
	}
	return nil
}

func (r *MariaDBReconciler) handleInitialBackup(ctx context.Context, mariadb *mariadbv1alpha1.MariaDB,
	replication mariadbv1alpha1.Replication, logger logr.Logger) error {
	logger.Info("Reconciling initial logical backup for external replication")

	emdb, err := r.RefResolver.ExternalMariaDB(ctx, &replication.ReplicaFromExternal.MariaDBRef, mariadb.Namespace)
	if err != nil {
		return fmt.Errorf("error getting external MariaDB object: %v", err)
	}
	key := types.NamespacedName{
		Name:      emdb.Name,
		Namespace: emdb.Namespace,
	}

	logger.Info("Checking if viable backup already exists")
	var isBackupInvalid = false
	var binlogExpireLogsDuration time.Duration
	var existingBackup mariadbv1alpha1.Backup

	logger.Info("Getting the binlog_expire_logs_seconds on the external MariaDB")
	if binlogExpireLogsDuration, err = getBinlogExpireLogsDuration(emdb, ctx, r.RefResolver); err != nil {
		return fmt.Errorf("unable to get binlog_expire_logs_seconds: %v", err)
	}

	logger.Info("Trying to get the current backup")
	err = r.Get(ctx, key, &existingBackup)

	if err == nil {
		logger.Info("Backup exists, check if it is expired")
		isBackupInvalid = removeBackupIfExpired(existingBackup, ctx, binlogExpireLogsDuration, *r)
	}
	// Create a new backup if required
	if err != nil || isBackupInvalid {
		logger.Info("Take a new backup")
		return newBackup(emdb, *r, ctx, binlogExpireLogsDuration, mariadb.GetImagePullSecrets(), mariadb.Spec.Storage.Size)
	}

	if !existingBackup.IsComplete() {
		logger.Info("Backup is running")
		return fmt.Errorf("backup still running")
	}

	return nil
}

func (r *MariaDBReconciler) replicationPodIndexes(mariadb *mariadbv1alpha1.MariaDB) []int {
	podIndexes := []int{
		*mariadb.Status.CurrentPrimaryPodIndex,
	}
	for i := 0; i < int(mariadb.Spec.Replicas); i++ {
		if i != *mariadb.Status.CurrentPrimaryPodIndex {
			podIndexes = append(podIndexes, i)
		}
	}
	return podIndexes
}

func removeBackupIfExpired(existingBackup mariadbv1alpha1.Backup, ctx context.Context,
	binlogExpireLogsDuration time.Duration, r MariaDBReconciler) bool {
	if time.Since(existingBackup.CreationTimestamp.Time) > binlogExpireLogsDuration {
		if err := r.Delete(ctx, &existingBackup); err == nil {
			return true
		}
	}
	return false
}

func newBackup(emdb *mariadbv1alpha1.ExternalMariaDB, r MariaDBReconciler, ctx context.Context,
	binlogExpireLogsDuration time.Duration, imagePullSecrets []mariadbv1alpha1.LocalObjectReference,
	size *resource.Quantity) error {

	key := types.NamespacedName{
		Name:      emdb.Name,
		Namespace: emdb.Namespace,
	}
	backupOps := builder.BackupOpts{
		Metadata: []*mariadbv1alpha1.Metadata{emdb.Spec.InheritMetadata},
		Key:      key,
		MariaDBRef: mariadbv1alpha1.MariaDBRef{
			ObjectReference: mariadbv1alpha1.ObjectReference{
				Name: emdb.Name,
			},
			Kind: mariadbv1alpha1.ExternalMariaDBKind,
		},
		Args: []string{
			"--master-data=1",
			"--gtid",
			"--verbose",
			"--all-databases",
			"--single-transaction",
			"--ignore-table=mysql.global_priv",
		},
		Compression: mariadbv1alpha1.CompressGzip,
		Storage: mariadbv1alpha1.BackupStorage{
			PersistentVolumeClaim: &mariadbv1alpha1.PersistentVolumeClaimSpec{
				AccessModes: []corev1.PersistentVolumeAccessMode{
					corev1.ReadWriteOnce,
				},
				Resources: corev1.VolumeResourceRequirements{
					Requests: corev1.ResourceList{
						"storage": *size,
					},
				},
			},
		},
		MaxRetention:     binlogExpireLogsDuration,
		ImagePullSecrets: imagePullSecrets,
	}

	backup, err := r.Builder.BuildBackup(backupOps, emdb)
	if err != nil {
		return fmt.Errorf("error building Backup object: %v", err)
	}
	if err := r.Create(ctx, backup); err != nil {
		return fmt.Errorf("error creating base Backup: %v", err)
	}
	return nil
}

func getBinlogExpireLogsDuration(emdb *mariadbv1alpha1.ExternalMariaDB, ctx context.Context,
	refResolver *refresolver.RefResolver) (time.Duration, error) {
	var external_client *sql.Client
	var err error
	if external_client, err = sql.NewClientWithMariaDB(ctx, emdb, refResolver); err != nil {
		return time.Duration(0), fmt.Errorf("error getting external MariaDB client: %v", err)
	}
	defer external_client.Close()

	var binlogExpireLogsSecondsStr string
	var binlogExpireLogsSeconds int

	if semver.Compare(emdb.Status.Version, "10.6.1") >= 0 {
		binlogExpireLogsSecondsStr, err = external_client.SystemVariable(ctx, "binlog_expire_logs_seconds")
		if err != nil {
			return time.Duration(0), fmt.Errorf("unable to get binlog_expire_logs_seconds: %v", err)
		}
		binlogExpireLogsSeconds, _ = strconv.Atoi(binlogExpireLogsSecondsStr)
	} else {
		binlogExpireLogsDaysStr, err := external_client.SystemVariable(ctx, "binlog_expire_logs_seconds")
		if err != nil {
			return time.Duration(0), fmt.Errorf("unable to get binlog_expire_logs_seconds: %v", err)
		}
		binlogExpireLogsDays, _ := strconv.Atoi(binlogExpireLogsDaysStr)
		binlogExpireLogsSeconds = binlogExpireLogsDays * 86400
	}

	return time.Duration(binlogExpireLogsSeconds) * time.Second, nil
}

func newRestore(mariadb *mariadbv1alpha1.MariaDB, r MariaDBReconciler, ctx context.Context, replicaPodIndex int) error {
	restoreOpts := builder.RestoreOpts{
		PodIndex: &replicaPodIndex,
	}
	restore, err := r.Builder.BuildRestore(mariadb, mariadb.RestoreKeyInPod(replicaPodIndex), restoreOpts)
	if err != nil {
		return fmt.Errorf("error building Restore object: %v", err)
	}
	if err := r.Create(ctx, restore); err != nil {
		return fmt.Errorf("error creating Restore object: %v", err)
	}
	// return fmt.Errorf("CREATING Restore object: %v", restore.Name)
	return nil
}

// func (r *MariaDBReconciler) isScalingOut(mdb *mariadbv1alpha1.MariaDB, sts *appsv1.StatefulSet) (bool, error) {
// 	if !mdb.IsReplicationEnabled() || !mdb.HasConfiguredReplication() || sts.Status.Replicas == 0 {
// 		return false, nil
// 	}
// 	if mdb.IsSwitchingPrimary() || mdb.IsReplicationSwitchoverRequired() || mdb.IsInitializing() || mdb.IsRecoveringReplicas() ||
// 		mdb.IsRestoringBackup() || mdb.IsResizingStorage() || mdb.IsUpdating() {
// 		return false, nil
// 	}
// 	// user is able to rollback scale out operation at any point by matching the number of existing replicas
// 	if sts.Status.Replicas == mdb.Spec.Replicas {
// 		return false, nil
// 	}
// 	// ongoing scale out process
// 	if mdb.IsScalingOut() {
// 		return true, nil
// 	}
// 	// initial condition for starting scale out process, all replicas should be ready
// 	return sts.Status.Replicas == sts.Status.ReadyReplicas &&
// 		sts.Status.Replicas < mdb.Spec.Replicas, nil
// }

// func (r *MariaDBReconciler) reconcileScaleOutError(ctx context.Context, mariadb *mariadbv1alpha1.MariaDB, fromIndex int,
// 	logger logr.Logger) (ctrl.Result, error) {
// 	replication := ptr.Deref(mariadb.Spec.Replication, mariadbv1alpha1.Replication{})

// 	if replication.Replica.ReplicaBootstrapFrom == nil {
// 		r.Recorder.Eventf(mariadb, corev1.EventTypeWarning, mariadbv1alpha1.ReasonMariaDBScaleOutError,
// 			"Unable to scale out MariaDB: replica datasource not found (replication.replica.bootstrapFrom is nil)")

// 		if err := r.patchStatus(ctx, mariadb, func(status *mariadbv1alpha1.MariaDBStatus) error {
// 			condition.SetScaleOutError(status, "replica datasource not found (replication.replica.bootstrapFrom is nil)")
// 			return nil
// 		}); err != nil {
// 			return ctrl.Result{}, fmt.Errorf("error patching MariaDB status: %v", err)
// 		}

// 		logger.Info("Unable to scale out MariaDB: replica datasource not found (replication.replica.bootstrapFrom is nil). Requeuing...")
// 		return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
// 	}

// 	pvcsAlreadyExist, err := r.pvcAlreadyExists(ctx, mariadb, fromIndex)
// 	if err != nil {
// 		return ctrl.Result{}, fmt.Errorf("error checking PVCs: %v", err)
// 	}
// 	if pvcsAlreadyExist {
// 		r.Recorder.Eventf(mariadb, corev1.EventTypeWarning, mariadbv1alpha1.ReasonMariaDBScaleOutError,
// 			"Unable to scale out MariaDB: storage PVCs already exist")

// 		if err := r.patchStatus(ctx, mariadb, func(status *mariadbv1alpha1.MariaDBStatus) error {
// 			condition.SetScaleOutError(status, "storage PVCs already exist")
// 			return nil
// 		}); err != nil {
// 			return ctrl.Result{}, fmt.Errorf("error patching MariaDB status: %v", err)
// 		}

// 		logger.Info("Unable to scale out MariaDB: storage PVCs already exist. Requeuing...")
// 		return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
// 	}

// 	return ctrl.Result{}, nil
// }

// func (r *MariaDBReconciler) pvcAlreadyExists(ctx context.Context, mariadb *mariadbv1alpha1.MariaDB, fromIndex int) (bool, error) {
// 	for i := fromIndex; i < int(mariadb.Spec.Replicas); i++ {
// 		pvcKey := mariadb.PVCKey(builder.StorageVolumeRole, i)
// 		var pvc corev1.PersistentVolumeClaim
// 		err := r.Get(ctx, pvcKey, &pvc)
// 		if err == nil {
// 			return true, nil
// 		}
// 		if !apierrors.IsNotFound(err) {
// 			return false, fmt.Errorf("error getting PVC %s: %v", pvcKey.Name, err)
// 		}
// 	}
// 	return false, nil
// }

// func (r *MariaDBReconciler) reconcileReplicaPhysicalBackup(ctx context.Context, key types.NamespacedName, mariadb *mariadbv1alpha1.MariaDB,
// 	logger logr.Logger) (ctrl.Result, error) {
// 	var physicalBackup mariadbv1alpha1.PhysicalBackup
// 	if err := r.Get(ctx, key, &physicalBackup); err != nil {
// 		if apierrors.IsNotFound(err) {
// 			logger.Info("Creating PhysicalBackup", "name", key.Name)
// 			if err := r.createReplicaPhysicalBackup(ctx, key, mariadb); err != nil {
// 				return ctrl.Result{}, err
// 			}
// 		}
// 		return ctrl.Result{RequeueAfter: 1 * time.Second}, nil
// 	}
// 	if !physicalBackup.IsComplete() {
// 		logger.V(1).Info("Replica PhysicalBackup job not completed. Requeuing")
// 		return ctrl.Result{RequeueAfter: 1 * time.Second}, nil
// 	}
// 	return ctrl.Result{}, nil
// }

// func (r *MariaDBReconciler) createReplicaPhysicalBackup(ctx context.Context, key types.NamespacedName,
// 	mariadb *mariadbv1alpha1.MariaDB) error {
// 	replication := ptr.Deref(mariadb.Spec.Replication, mariadbv1alpha1.Replication{})
// 	if replication.Replica.ReplicaBootstrapFrom == nil {
// 		return errors.New("replica datasource not found")
// 	}

// 	tplKey := types.NamespacedName{
// 		Name:      replication.Replica.ReplicaBootstrapFrom.PhysicalBackupTemplateRef.Name,
// 		Namespace: mariadb.Namespace,
// 	}
// 	var tpl mariadbv1alpha1.PhysicalBackup
// 	if err := r.Get(ctx, tplKey, &tpl); err != nil {
// 		return fmt.Errorf("error getting PhysicalBackup template: %v", err)
// 	}

// 	physicalBackup, err := r.Builder.BuildReplicaRecoveryPhysicalBackup(key, &tpl, mariadb)
// 	if err != nil {
// 		return fmt.Errorf("error building PhysicalBackup: %v", err)
// 	}
// 	return r.Create(ctx, physicalBackup)
// }

// func (r *MariaDBReconciler) getPhysicalBackup(ctx context.Context, key types.NamespacedName,
// 	mariadb *mariadbv1alpha1.MariaDB) (*mariadbv1alpha1.PhysicalBackup, error) {
// 	var physicalBackup mariadbv1alpha1.PhysicalBackup
// 	if err := r.Get(ctx, key, &physicalBackup); err != nil {
// 		return nil, err
// 	}
// 	return &physicalBackup, nil
// }

// func (r *MariaDBReconciler) getVolumeSnapshotKey(ctx context.Context, mariadb *mariadbv1alpha1.MariaDB,
// 	physicalBackup *mariadbv1alpha1.PhysicalBackup) (*types.NamespacedName, error) {
// 	if physicalBackup.Spec.Storage.VolumeSnapshot == nil {
// 		return nil, nil
// 	}
// 	snapshotList, err := mdbsnapshot.ListVolumeSnapshots(ctx, r.Client, physicalBackup)
// 	if err != nil {
// 		return nil, err
// 	}
// 	if len(snapshotList.Items) == 0 {
// 		return nil, errors.New("VolumeSnapshot not found")
// 	}
// 	sort.Slice(snapshotList.Items, func(i, j int) bool {
// 		return snapshotList.Items[i].CreationTimestamp.After(snapshotList.Items[j].CreationTimestamp.Time)
// 	})
// 	return ptr.To(client.ObjectKeyFromObject(&snapshotList.Items[0])), nil
// }

// func (r *MariaDBReconciler) setScaledOutAndCleanup(ctx context.Context, mariadb *mariadbv1alpha1.MariaDB,
// 	logger logr.Logger) (ctrl.Result, error) {
// 	logger.Info("Scale out and cleanup")
// 	if !mariadb.IsScalingOut() {
// 		logger.Info("Not scaling out")
// 		return ctrl.Result{}, nil
// 	}
// 	physicalBackupKey := mariadb.PhysicalBackupScaleOutKey()
// 	logger.Info("physical backup", "key", physicalBackupKey)

// 	if mariadb.Status.ScaleOutInitialIndex != nil {
// 		logger.Info("Scale out initial index", "index", *mariadb.Status.ScaleOutInitialIndex)
// 		fromIndex := *mariadb.Status.ScaleOutInitialIndex

// 		physicalBackup, err := r.getPhysicalBackup(ctx, physicalBackupKey, mariadb)
// 		if err != nil {
// 			return ctrl.Result{}, fmt.Errorf("error getting PhysicalBackup: %v", err)
// 		}
// 		snapshotKey, err := r.getVolumeSnapshotKey(ctx, mariadb, physicalBackup)
// 		if err != nil {
// 			return ctrl.Result{}, fmt.Errorf("error getting VolumeSnapshot key: %v", err)
// 		}

// 		if err := r.ensureReplicationConfigured(ctx, fromIndex, mariadb, snapshotKey, logger); err != nil {
// 			return ctrl.Result{}, err
// 		}

// 		if err := r.patchStatus(ctx, mariadb, func(status *mariadbv1alpha1.MariaDBStatus) error {
// 			status.ScaleOutInitialIndex = nil
// 			return nil
// 		}); err != nil {
// 			return ctrl.Result{}, fmt.Errorf("error patching MariaDB status: %v", err)
// 		}
// 		// Requeue to track replication status
// 		if mariadb.IsReplicationEnabled() {
// 			return ctrl.Result{Requeue: true}, nil
// 		}
// 	}

// 	if err := r.patchStatus(ctx, mariadb, func(status *mariadbv1alpha1.MariaDBStatus) error {
// 		condition.SetScaledOut(status)
// 		status.ScaleOutInitialIndex = nil
// 		return nil
// 	}); err != nil {
// 		return ctrl.Result{}, fmt.Errorf("error patching MariaDB status: %v", err)
// 	}

// 	if err := r.cleanupPhysicalBackup(ctx, physicalBackupKey); err != nil {
// 		return ctrl.Result{}, err
// 	}
// 	if err := r.cleanupInitJobs(ctx, mariadb); err != nil {
// 		return ctrl.Result{}, err
// 	}
// 	return ctrl.Result{}, nil
// }

// func (r *MariaDBReconciler) cleanupPhysicalBackup(ctx context.Context, key types.NamespacedName) error {
// 	var physicalBackup mariadbv1alpha1.PhysicalBackup
// 	if err := r.Get(ctx, key, &physicalBackup); err != nil {
// 		if apierrors.IsNotFound(err) {
// 			return nil
// 		}
// 		return err
// 	}
// 	return r.Delete(ctx, &physicalBackup)
// }
