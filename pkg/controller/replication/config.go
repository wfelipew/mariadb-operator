package replication

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"html/template"
	"strconv"
	"strings"

	"github.com/go-logr/logr"
	mariadbv1alpha1 "github.com/mariadb-operator/mariadb-operator/v25/api/v1alpha1"
	"github.com/mariadb-operator/mariadb-operator/v25/pkg/builder"
	builderpki "github.com/mariadb-operator/mariadb-operator/v25/pkg/builder/pki"
	"github.com/mariadb-operator/mariadb-operator/v25/pkg/controller/secret"
	env "github.com/mariadb-operator/mariadb-operator/v25/pkg/environment"
	"github.com/mariadb-operator/mariadb-operator/v25/pkg/refresolver"
	"github.com/mariadb-operator/mariadb-operator/v25/pkg/sql"
	"github.com/mariadb-operator/mariadb-operator/v25/pkg/statefulset"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

var (
	replUser     = "repl"
	replUserHost = "%"
)

type ReplicationConfigClient struct {
	client.Client
	builder          *builder.Builder
	refResolver      *refresolver.RefResolver
	secretReconciler *secret.SecretReconciler
}

func NewReplicationConfigClient(client client.Client, builder *builder.Builder,
	secretReconciler *secret.SecretReconciler) *ReplicationConfigClient {
	return &ReplicationConfigClient{
		Client:           client,
		builder:          builder,
		refResolver:      refresolver.New(client),
		secretReconciler: secretReconciler,
	}
}

func (r *ReplicationConfigClient) ConfigurePrimary(ctx context.Context, mariadb *mariadbv1alpha1.MariaDB, client *sql.Client) error {
	isReplica, err := client.IsReplicationReplica(ctx)
	if err != nil {
		return fmt.Errorf("error checking replica: %v", err)
	}
	if isReplica {
		if err := client.StopAllSlaves(ctx); err != nil {
			return fmt.Errorf("error stopping slaves: %v", err)
		}
		if err := client.ResetAllSlaves(ctx); err != nil {
			return fmt.Errorf("error resetting slave: %v", err)
		}
		if err := client.ResetGtidSlavePos(ctx); err != nil {
			return fmt.Errorf("error resetting slave position: %v", err)
		}
	}
	if err := client.DisableReadOnly(ctx); err != nil {
		return fmt.Errorf("error disabling read_only: %v", err)
	}
	if err := r.reconcilePrimarySql(ctx, mariadb, client); err != nil {
		return fmt.Errorf("error reconciling primary SQL: %v", err)
	}
	return nil
}

type ConfigureReplicaOpts struct {
	GtidSlavePos      *string
	ResetGtidSlavePos bool
	ChangeMasterOpts  []sql.ChangeMasterOpt
}

type ConfigureReplicaOpt func(*ConfigureReplicaOpts)

func WithGtidSlavePos(gtid string) ConfigureReplicaOpt {
	return func(cro *ConfigureReplicaOpts) {
		cro.GtidSlavePos = &gtid
	}
}

func WithResetGtidSlavePos() ConfigureReplicaOpt {
	return func(cro *ConfigureReplicaOpts) {
		cro.ResetGtidSlavePos = true
	}
}

func WithChangeMasterOpts(opts ...sql.ChangeMasterOpt) ConfigureReplicaOpt {
	return func(cro *ConfigureReplicaOpts) {
		cro.ChangeMasterOpts = opts
	}
}

func (r *ReplicationConfigClient) ConfigureReplica(ctx context.Context, mariadb *mariadbv1alpha1.MariaDB, client *sql.Client,
	primaryPodIndex int, replicaPodIndex int, replicaOpts ...ConfigureReplicaOpt) error {

	opts := ConfigureReplicaOpts{}
	for _, setOpt := range replicaOpts {
		setOpt(&opts)
	}

	// replication := mariadb.Replication()
	// isExternalReplication := replication.IsExternalReplication()

	if err := client.ResetMaster(ctx); err != nil {
		return fmt.Errorf("error resetting master: %v", err)
	}
	if err := client.StopAllSlaves(ctx); err != nil {
		return fmt.Errorf("error stopping slaves: %v", err)
	}
	if opts.GtidSlavePos != nil {
		if err := client.SetGtidSlavePos(ctx, *opts.GtidSlavePos); err != nil {
			return fmt.Errorf("error setting slave position \"%s\": %v", *opts.GtidSlavePos, err)
		}
	} else if opts.ResetGtidSlavePos {
		if err := client.ResetGtidSlavePos(ctx); err != nil {
			return fmt.Errorf("error resetting slave position: %v", err)
		}
	}
	if err := client.EnableReadOnly(ctx); err != nil {
		return fmt.Errorf("error enabling read_only: %v", err)
	}

	// isReplicationConfigured, _ := client.IsReplicationConfigured(ctx)

	// if isExternalReplication && !isReplicationConfigured {

	// 	if ready, err := r.configureExternalReplica(ctx, mariadb, replicaPodIndex); !ready || err != nil {
	// 		return err
	// 	}
	// }

	if err := r.changeMaster(ctx, mariadb, client, primaryPodIndex, opts.ChangeMasterOpts...); err != nil {
		return fmt.Errorf("error changing master: %v", err)
	}
	if err := client.StartSlave(ctx); err != nil {
		return fmt.Errorf("error starting slave: %v", err)
	}
	return nil
}

// func (r *ReplicationConfigClient) configureExternalReplica(ctx context.Context, mariadb *mariadbv1alpha1.MariaDB,
// 	replicaPodIndex int) (bool, error) {

// 	return true, nil
// }

func (r *ReplicationConfigClient) changeMaster(ctx context.Context, mariadb *mariadbv1alpha1.MariaDB, client *sql.Client,
	primaryPodIndex int, opts ...sql.ChangeMasterOpt) error {
	replication := ptr.Deref(mariadb.Spec.Replication, mariadbv1alpha1.Replication{})

	if replication.Replica.ReplPasswordSecretKeyRef == nil {
		return errors.New("'spec.replication.replica.replPasswordSecretKeyRef` must not be nil'")
	}

	password, err := r.refResolver.SecretKeyRef(ctx, replication.Replica.ReplPasswordSecretKeyRef.SecretKeySelector, mariadb.Namespace)
	if err != nil {
		return fmt.Errorf("error getting replication password: %v", err)
	}

	gtid := ptr.Deref(replication.Replica.Gtid, mariadbv1alpha1.GtidCurrentPos)
	gtidString, err := gtid.MariaDBFormat()
	if err != nil {
		return fmt.Errorf("error getting change master GTID: %v", err)
	}

	var changeMasterOpts []sql.ChangeMasterOpt

	if !replication.IsExternalReplication() {
		changeMasterOpts = []sql.ChangeMasterOpt{
			sql.WithChangeMasterHost(
				statefulset.PodFQDNWithService(
					mariadb.ObjectMeta,
					primaryPodIndex,
					mariadb.InternalServiceKey().Name,
				),
			),
			sql.WithChangeMasterPort(mariadb.Spec.Port),
			sql.WithChangeMasterCredentials(replUser, password),
			sql.WithChangeMasterGtid(gtidString),
		}
		if mariadb.IsTLSEnabled() {
			changeMasterOpts = append(changeMasterOpts, sql.WithChangeMasterSSL(
				builderpki.ClientCertPath,
				builderpki.ClientKeyPath,
				builderpki.CACertPath,
			))
		}

		if retries := ptr.Deref(replication.Replica.ConnectionRetrySeconds, -1); retries != -1 {
			changeMasterOpts = append(changeMasterOpts, sql.WithChangeMasterRetries(*replication.Replica.ConnectionRetrySeconds))
		}

		changeMasterOpts = append(changeMasterOpts, opts...)
	} else {
		var emdb *mariadbv1alpha1.ExternalMariaDB
		replPasswordRef, err := externalReplPasswordRef(mariadb, r.refResolver, ctx)
		if err != nil {
			return fmt.Errorf("error getting ExternalMariaDB password Ref: %v", err)
		}
		password, err := r.refResolver.SecretKeyRef(ctx, replPasswordRef, mariadb.Namespace)
		if err != nil {
			return fmt.Errorf("error getting ExternalMariaDB password replication secret: %v", err)
		}
		emdbRef := replication.GetExternalReplicationRef()
		emdb, err = r.refResolver.ExternalMariaDB(ctx, &emdbRef, mariadb.Namespace)
		if err != nil {
			return fmt.Errorf("error getting ExternalMariaDB: %v", err)
		}
		changeMasterOpts = []sql.ChangeMasterOpt{
			sql.WithChangeMasterHost(
				emdb.GetHost(),
			),
			sql.WithChangeMasterCredentials(emdb.GetSUName(), password),
		}
		if emdb.GetBinlogProxyPort() != nil {
			changeMasterOpts = append(changeMasterOpts, sql.WithChangeMasterPort(*emdb.GetBinlogProxyPort()))
		} else {
			changeMasterOpts = append(changeMasterOpts, sql.WithChangeMasterPort(emdb.GetPort()))
		}
	}

	if err := client.ChangeMaster(ctx, changeMasterOpts...); err != nil {
		return fmt.Errorf("error changing master: %v", err)
	}
	return nil
}

// externalMasterEndpoint resolves the master connection details (host, port and user) that an
// external replica should currently be replicating from, reading them from the referenced
// ExternalMariaDB resource.
func (r *ReplicationConfigClient) externalMasterEndpoint(ctx context.Context,
	mariadb *mariadbv1alpha1.MariaDB) (host string, port int32, user string, err error) {
	replication := mariadb.Replication()
	emdbRef := replication.GetExternalReplicationRef()
	emdb, err := r.refResolver.ExternalMariaDB(ctx, &emdbRef, mariadb.Namespace)
	if err != nil {
		return "", 0, "", fmt.Errorf("error getting ExternalMariaDB: %v", err)
	}
	port = emdb.GetPort()
	if emdb.GetBinlogProxyPort() != nil {
		port = *emdb.GetBinlogProxyPort()
	}
	return emdb.GetHost(), port, emdb.GetSUName(), nil
}

// replicaAuthErrnos are the IO thread error codes MariaDB reports when the replica cannot
// authenticate against the master, e.g. after the replication password has been rotated.
var replicaAuthErrnos = map[int32]struct{}{
	1045: {}, // ER_ACCESS_DENIED_ERROR
	1698: {}, // ER_ACCESS_DENIED_NO_PASSWORD_ERROR
}

// isReplicaAuthError reports whether the replica IO thread is failing to authenticate against
// the master. The password configured on the replica cannot be read back, so an access-denied
// error is our only signal that a rotated password needs to be re-applied.
func isReplicaAuthError(ioRunning, lastIOErrno, lastIOError string) bool {
	if ioRunning == "Yes" {
		return false
	}
	if errno, err := strconv.Atoi(lastIOErrno); err == nil {
		if _, ok := replicaAuthErrnos[int32(errno)]; ok {
			return true
		}
	}
	return strings.Contains(strings.ToLower(lastIOError), "access denied")
}

// ReconcileExternalReplicaDrift re-points a replica that is already configured for external
// replication at the current ExternalMariaDB connection details when it has drifted.
//
// It repairs in two cases:
//   - The configured master host, port or user no longer matches the ExternalMariaDB endpoint.
//   - The replica IO thread is failing with an authentication error. The configured password
//     cannot be read back, so re-issuing CHANGE MASTER (which always re-sends the current secret
//     value) is how a rotated replication password gets applied.
//
// Unlike ConfigureReplica, this performs a minimal, non-destructive repair: it does NOT reset the
// master nor require a GTID position. It only stops the slave threads (when they are running),
// issues CHANGE MASTER with the updated connection details (keeping MASTER_USE_GTID=current_pos)
// and starts the slave again. It returns true when a repair was performed.
func (r *ReplicationConfigClient) ReconcileExternalReplicaDrift(ctx context.Context, mariadb *mariadbv1alpha1.MariaDB,
	client *sql.Client, primaryPodIndex int, logger logr.Logger) (bool, error) {
	desiredHost, desiredPort, desiredUser, err := r.externalMasterEndpoint(ctx, mariadb)
	if err != nil {
		return false, fmt.Errorf("error getting external master endpoint: %v", err)
	}

	// Read SHOW REPLICA STATUS as a column map rather than via a positional scan so the check is
	// resilient to column ordering changes across MariaDB versions.
	status, err := client.QueryColumnMap(ctx, "SHOW REPLICA STATUS")
	if err != nil {
		return false, fmt.Errorf("error getting replica status: %v", err)
	}
	currentHost := status["Master_Host"]
	currentPort := status["Master_Port"]
	currentUser := status["Master_User"]
	ioRunning := status["Slave_IO_Running"]
	sqlRunning := status["Slave_SQL_Running"]

	endpointDrift := currentHost != desiredHost ||
		currentPort != strconv.Itoa(int(desiredPort)) ||
		currentUser != desiredUser
	authError := isReplicaAuthError(ioRunning, status["Last_IO_Errno"], status["Last_IO_Error"])

	if !endpointDrift && !authError {
		return false, nil
	}

	if endpointDrift {
		logger.Info("external replica master drift detected, repairing",
			"current-host", currentHost, "current-port", currentPort, "current-user", currentUser,
			"desired-host", desiredHost, "desired-port", desiredPort, "desired-user", desiredUser)
	}
	if authError {
		logger.Info("external replica authentication error detected, re-applying credentials",
			"last-io-errno", status["Last_IO_Errno"], "last-io-error", status["Last_IO_Error"])
	}

	if ioRunning != "No" || sqlRunning != "No" {
		if err := client.StopAllSlaves(ctx); err != nil {
			return false, fmt.Errorf("error stopping slaves: %v", err)
		}
	}
	if err := r.changeMaster(ctx, mariadb, client, primaryPodIndex); err != nil {
		return false, fmt.Errorf("error changing master: %v", err)
	}
	if err := client.StartSlave(ctx); err != nil {
		return false, fmt.Errorf("error starting slave: %v", err)
	}
	return true, nil
}

func (r *ReplicationConfigClient) reconcilePrimarySql(ctx context.Context, mariadb *mariadbv1alpha1.MariaDB, client *sql.Client) error {
	opts := userSqlOpts{
		username:   replUser,
		host:       replUserHost,
		privileges: []string{"REPLICATION REPLICA"},
	}
	if err := r.reconcileUserSql(ctx, mariadb, client, &opts); err != nil {
		return fmt.Errorf("error reconciling '%s' SQL user: %v", replUser, err)
	}
	return nil
}

type userSqlOpts struct {
	username   string
	host       string
	privileges []string
}

func (r *ReplicationConfigClient) reconcileUserSql(ctx context.Context, mariadb *mariadbv1alpha1.MariaDB, client *sql.Client,
	opts *userSqlOpts) error {
	replication := ptr.Deref(mariadb.Spec.Replication, mariadbv1alpha1.Replication{})
	if replication.Replica.ReplPasswordSecretKeyRef == nil {
		return errors.New("'spec.replication.replica.replPasswordSecretKeyRef` must not be nil'")
	}

	replPassword, err := r.refResolver.SecretKeyRef(ctx, replication.Replica.ReplPasswordSecretKeyRef.SecretKeySelector, mariadb.Namespace)
	if err != nil {
		return fmt.Errorf("error getting repl password: %v", err)
	}
	accountName := formatAccountName(opts.username, opts.host)

	exists, err := client.UserExists(ctx, opts.username, opts.host)
	if err != nil {
		return fmt.Errorf("error checking if replication user exists: %v", err)
	}
	if exists {
		if err := client.AlterUser(ctx, accountName, sql.WithIdentifiedBy(replPassword)); err != nil {
			return fmt.Errorf("error altering replication user: %v", err)
		}
	} else {
		if err := client.CreateUser(ctx, accountName, sql.WithIdentifiedBy(replPassword)); err != nil {
			return fmt.Errorf("error creating replication user: %v", err)
		}
	}
	if err := client.Grant(
		ctx,
		opts.privileges,
		"*",
		"*",
		accountName,
	); err != nil {
		return fmt.Errorf("error creating grant: %v", err)
	}
	return nil
}

func NewReplicationConfig(env *env.PodEnvironment) ([]byte, error) {
	var sId int

	replEnabled, err := env.IsReplEnabled()
	if err != nil {
		return nil, fmt.Errorf("error checking if replication is enabled: %v", err)
	}
	if !replEnabled {
		return nil, errors.New("replication must be enabled")
	}
	gtidStrictMode, err := env.ReplGtidStrictMode()
	if err != nil {
		return nil, fmt.Errorf("error getting GTID strict mode: %v", err)
	}
	semiSyncEnabled, err := env.ReplSemiSyncEnabled()
	if err != nil {
		return nil, fmt.Errorf("error getting semi-sync enabled: %v", err)
	}
	semiSyncMasterTimeout, err := env.ReplSemiSyncMasterTimeout()
	if err != nil {
		return nil, fmt.Errorf("error getting semi-sync master timeout: %v", err)
	}
	externalReplEnabled, err := env.IsExternalReplEnabled()
	if err != nil {
		return nil, fmt.Errorf("error checking if external replication is enabled: %v", err)
	}

	externalReplServerIdOffset, err := env.ExternalReplServerIdOffset()
	if err != nil {
		return nil, fmt.Errorf("error get serverId offset for external replication: %v", err)
	}

	if externalReplEnabled && externalReplServerIdOffset != nil {
		sId, err = offsetServerId(env.PodName, *externalReplServerIdOffset)
		if err != nil {
			return nil, fmt.Errorf("error getting server_id with offset server ID: %v", err)
		}
	} else {
		sId, err = serverId(env.PodName)
		if err != nil {
			return nil, fmt.Errorf("error getting server ID: %v", err)
		}
	}

	syncBinlog, err := env.ReplSyncBinlog()
	if err != nil {
		return nil, fmt.Errorf("error getting master sync binlog: %v", err)
	}

	filteredTables := env.ExternalReplFilteredTables()

	// To facilitate switchover/failover and avoid clashing with MaxScale, this configuration allows any Pod to act either as a primary or a replica.
	// See: https://mariadb.com/docs/server/ha-and-performance/standard-replication/semisynchronous-replication#enabling-semisynchronous-replication
	tpl := createTpl("replication", `[mariadb]
log_bin
log_basename={{.LogName }}
{{- with .GtidStrictMode }}
gtid_strict_mode
{{- end }}
{{- if .SemiSyncEnabled }}
rpl_semi_sync_master_enabled=ON
rpl_semi_sync_slave_enabled=ON
{{- with .SemiSyncMasterTimeout }}
rpl_semi_sync_master_timeout={{ . }}
{{- end }}
{{- with .SemiSyncMasterWaitPoint }}
rpl_semi_sync_master_wait_point={{ . }}
{{- end }}
{{- end }}
server_id={{ .ServerId }}
{{- with .SyncBinlog }}
sync_binlog={{ . }}
{{- end }}
{{- range .ReplicateDoTables }}
replicate_do_table={{ . }}
{{- end }}
`)
	buf := new(bytes.Buffer)
	err = tpl.Execute(buf, struct {
		LogName                 string
		GtidStrictMode          bool
		SemiSyncEnabled         bool
		SemiSyncMasterTimeout   *int64
		SemiSyncMasterWaitPoint string
		SyncBinlog              *int
		ServerId                int
		ReplicateDoTables       []string
	}{
		LogName:                 env.MariadbName,
		GtidStrictMode:          gtidStrictMode,
		SemiSyncEnabled:         semiSyncEnabled,
		SemiSyncMasterTimeout:   semiSyncMasterTimeout,
		SemiSyncMasterWaitPoint: env.MariaDBReplSemiSyncMasterWaitPoint,
		ServerId:                sId,
		SyncBinlog:              syncBinlog,
		ReplicateDoTables:       filteredTables,
	})
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func serverId(podName string) (int, error) {
	podIndex, err := statefulset.PodIndex(podName)
	if err != nil {
		return 0, fmt.Errorf("error getting Pod index: %v", err)
	}
	return 10 + *podIndex, nil
}

func externalReplPasswordRef(mariadb *mariadbv1alpha1.MariaDB, r *refresolver.RefResolver,
	ctx context.Context) (mariadbv1alpha1.SecretKeySelector, error) {
	replication := mariadb.Replication()
	// if mariadb.Replication().Enabled && mariadb.Replication().Replica.ReplPasswordSecretKeyRef != nil {
	// 	return mariadb.Replication().Replica.ReplPasswordSecretKeyRef.SecretKeySelector, nil
	// }
	if replication.IsExternalReplication() {
		emdbRef := replication.GetExternalReplicationRef()
		emdb, err := r.ExternalMariaDB(ctx, &emdbRef, mariadb.Namespace)
		if err == nil {
			return *emdb.GetSUCredential(), nil
		}
	}
	return mariadbv1alpha1.SecretKeySelector{
		LocalObjectReference: mariadbv1alpha1.LocalObjectReference{
			Name: "",
		},
		Key: "",
	}, fmt.Errorf("not able to get PasswordRef for external replication")
}

// func serverId(index int) string {
// 	return fmt.Sprint(10 + index)
// }

func offsetServerId(podName string, offset int) (int, error) {
	podIndex, err := statefulset.PodIndex(podName)
	if err != nil {
		return 0, fmt.Errorf("error getting Pod index: %v", err)
	}
	return *podIndex + offset, nil
}

func formatAccountName(username, host string) string {
	return fmt.Sprintf("'%s'@'%s'", username, host)
}

func createTpl(name, t string) *template.Template {
	return template.Must(template.New(name).Parse(t))
}
