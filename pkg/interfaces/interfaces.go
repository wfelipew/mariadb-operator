package interfaces

import (
	mariadbv1alpha1 "github.com/mariadb-operator/mariadb-operator/api/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clientpkg "sigs.k8s.io/controller-runtime/pkg/client"
)

type ImageAwareInterface interface {
	GetImagePullPolicy() corev1.PullPolicy
	GetImagePullSecrets() []mariadbv1alpha1.LocalObjectReference
	GetImage() string
}

type TLSAwareInterface interface {
	IsTLSEnabled() bool
	TLSCABundleSecretKeyRef() mariadbv1alpha1.SecretKeySelector
	TLSClientCertSecretKey() types.NamespacedName
	TLSServerCertSecretKey() types.NamespacedName
}

type ReplicationAwareInterface interface {
	GetReplicas() int32
	IsHAEnabled() bool
}

type ConnectionParamsAwareInterface interface {
	GetHost() string
	GetPodHost(podIndex int) string
	GetPort() int32
	GetSUName() *string
	GetSUCredential() mariadbv1alpha1.SecretKeySelector
}

type GaleraAwareInterface interface {
	IsGaleraEnabled() bool
}

type MariaDBGenericInterface interface {
	TLSAwareInterface
	ImageAwareInterface
	ConnectionParamsAwareInterface
	GaleraAwareInterface
	ReplicationAwareInterface
	runtime.Object
	IsReady() bool
	clientpkg.Object
}
