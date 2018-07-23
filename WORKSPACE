load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive")

http_archive(
    name = "io_bazel_rules_docker",
    sha256 = "6dede2c65ce86289969b907f343a1382d33c14fbce5e30dd17bb59bb55bb6593",
    strip_prefix = "rules_docker-0.4.0",
    urls = ["https://github.com/bazelbuild/rules_docker/archive/v0.4.0.tar.gz"],
)

http_archive(
    name = "io_bazel_rules_go",
    urls = ["https://github.com/bazelbuild/rules_go/releases/download/0.13.0/rules_go-0.13.0.tar.gz"],
    sha256 = "ba79c532ac400cefd1859cbc8a9829346aa69e3b99482cd5a54432092cbc3933",
)

http_archive(
    name = "bazel_gazelle",
    urls = ["https://github.com/bazelbuild/bazel-gazelle/releases/download/0.13.0/bazel-gazelle-0.13.0.tar.gz"],
    sha256 = "bc653d3e058964a5a26dcad02b6c72d7d63e6bb88d94704990b908a1445b8758",
)

load(
    "@io_bazel_rules_docker//go:image.bzl",
    _go_image_repos = "repositories",
)

_go_image_repos()

load(
    "@io_bazel_rules_docker//container:container.bzl",
    "container_pull",
    container_repositories = "repositories",
)

# This is NOT needed when going through the language lang_image
# "repositories" function(s).
container_repositories()

container_pull(
  name = "go_base",
  registry = "gcr.io",
  repository = "distroless/base",
  # 'tag' is also supported, but digest is encouraged for reproducibility.
  digest = "sha256:4b5f2a19a3cb56058265e400662df44c13352d0cb78a21f4c652b6b4d0159bb8",
)
load("@io_bazel_rules_go//go:def.bzl", "go_rules_dependencies", "go_register_toolchains")

go_rules_dependencies()

go_register_toolchains()

load("@bazel_gazelle//:deps.bzl", "gazelle_dependencies", "go_repository")

gazelle_dependencies()

go_repository(
    name = "com_github_apache_thrift",
    commit = "84d6af4cf903571319e0ebddd7beb12bc93fb752",
    importpath = "github.com/apache/thrift",
)

go_repository(
    name = "com_github_asaskevich_govalidator",
    commit = "ccb8e960c48f04d6935e72476ae4a51028f9e22f",
    importpath = "github.com/asaskevich/govalidator",
)

go_repository(
    name = "com_github_aws_aws_sdk_go",
    commit = "e4f914808a9655ef3220bb0082002239cc7f3561",
    importpath = "github.com/aws/aws-sdk-go",
)

go_repository(
    name = "com_github_beorn7_perks",
    commit = "3a771d992973f24aa725d07868b467d1ddfceafb",
    importpath = "github.com/beorn7/perks",
)

go_repository(
    name = "com_github_blang_semver",
    commit = "2ee87856327ba09384cabd113bc6b5d174e9ec0f",
    importpath = "github.com/blang/semver",
)

go_repository(
    name = "com_github_coreos_bbolt",
    commit = "48ea1b39c25fc1bab3506fbc712ecbaa842c4d2d",
    importpath = "github.com/coreos/bbolt",
)

go_repository(
    name = "com_github_coreos_etcd",
    commit = "33245c6b5b49130ca99280408fadfab01aac0e48",
    importpath = "github.com/coreos/etcd",
)

go_repository(
    name = "com_github_coreos_go_semver",
    commit = "8ab6407b697782a06568d4b7f1db25550ec2e4c6",
    importpath = "github.com/coreos/go-semver",
)

go_repository(
    name = "com_github_coreos_go_systemd",
    commit = "39ca1b05acc7ad1220e09f133283b8859a8b71ab",
    importpath = "github.com/coreos/go-systemd",
)

go_repository(
    name = "com_github_coreos_pkg",
    commit = "97fdf19511ea361ae1c100dd393cc47f8dcfa1e1",
    importpath = "github.com/coreos/pkg",
)

go_repository(
    name = "com_github_davecgh_go_spew",
    commit = "346938d642f2ec3594ed81d874461961cd0faa76",
    importpath = "github.com/davecgh/go-spew",
)

go_repository(
    name = "com_github_dgrijalva_jwt_go",
    commit = "06ea1031745cb8b3dab3f6a236daf2b0aa468b7e",
    importpath = "github.com/dgrijalva/jwt-go",
)

go_repository(
    name = "com_github_docker_distribution",
    commit = "48294d928ced5dd9b378f7fd7c6f5da3ff3f2c89",
    importpath = "github.com/docker/distribution",
)

go_repository(
    name = "com_github_dotmesh_io_citools",
    commit = "a433f673376c905b8e2f5b8c219a813dd3346c99",
    importpath = "github.com/dotmesh-io/citools",
)

go_repository(
    name = "com_github_dotmesh_io_go_checkpoint",
    commit = "edad9decbae8e2e2e58a64b18f7bd5efa76deb5a",
    importpath = "github.com/dotmesh-io/go-checkpoint",
)

go_repository(
    name = "com_github_eapache_go_resiliency",
    commit = "ea41b0fad31007accc7f806884dcdf3da98b79ce",
    importpath = "github.com/eapache/go-resiliency",
)

go_repository(
    name = "com_github_eapache_go_xerial_snappy",
    commit = "bb955e01b9346ac19dc29eb16586c90ded99a98c",
    importpath = "github.com/eapache/go-xerial-snappy",
)

go_repository(
    name = "com_github_eapache_queue",
    commit = "44cc805cf13205b55f69e14bcb69867d1ae92f98",
    importpath = "github.com/eapache/queue",
)

go_repository(
    name = "com_github_emicklei_go_restful",
    commit = "3eb9738c1697594ea6e71a7156a9bb32ed216cf0",
    importpath = "github.com/emicklei/go-restful",
)

go_repository(
    name = "com_github_emicklei_go_restful_swagger12",
    commit = "dcef7f55730566d41eae5db10e7d6981829720f6",
    importpath = "github.com/emicklei/go-restful-swagger12",
)

go_repository(
    name = "com_github_fsouza_go_dockerclient",
    commit = "b87634a9d98e8671b847597fb264953a3e0d02b5",
    importpath = "github.com/fsouza/go-dockerclient",
)

go_repository(
    name = "com_github_ghodss_yaml",
    commit = "0ca9ea5df5451ffdf184b4428c902747c2c11cd7",
    importpath = "github.com/ghodss/yaml",
)

go_repository(
    name = "com_github_go_ini_ini",
    commit = "06f5f3d67269ccec1fe5fe4134ba6e982984f7f5",
    importpath = "github.com/go-ini/ini",
)

go_repository(
    name = "com_github_go_openapi_analysis",
    commit = "ecce8cb68f3d9cdf720bd203b6de599fe03e5673",
    importpath = "github.com/go-openapi/analysis",
)

go_repository(
    name = "com_github_go_openapi_errors",
    commit = "b2b2befaf267d082d779bcef52d682a47c779517",
    importpath = "github.com/go-openapi/errors",
)

go_repository(
    name = "com_github_go_openapi_jsonpointer",
    commit = "3a0015ad55fa9873f41605d3e8f28cd279c32ab2",
    importpath = "github.com/go-openapi/jsonpointer",
)

go_repository(
    name = "com_github_go_openapi_jsonreference",
    commit = "3fb327e6747da3043567ee86abd02bb6376b6be2",
    importpath = "github.com/go-openapi/jsonreference",
)

go_repository(
    name = "com_github_go_openapi_loads",
    commit = "2a2b323bab96e6b1fdee110e57d959322446e9c9",
    importpath = "github.com/go-openapi/loads",
)

go_repository(
    name = "com_github_go_openapi_spec",
    commit = "bcff419492eeeb01f76e77d2ebc714dc97b607f5",
    importpath = "github.com/go-openapi/spec",
)

go_repository(
    name = "com_github_go_openapi_strfmt",
    commit = "481808443b00a14745fada967cb5eeff0f9b1df2",
    importpath = "github.com/go-openapi/strfmt",
)

go_repository(
    name = "com_github_go_openapi_swag",
    commit = "811b1089cde9dad18d4d0c2d09fbdbf28dbd27a5",
    importpath = "github.com/go-openapi/swag",
)

go_repository(
    name = "com_github_gogo_protobuf",
    commit = "1adfc126b41513cc696b209667c8656ea7aac67c",
    importpath = "github.com/gogo/protobuf",
)

go_repository(
    name = "com_github_golang_glog",
    commit = "23def4e6c14b4da8ac2ed8007337bc5eb5007998",
    importpath = "github.com/golang/glog",
)

go_repository(
    name = "com_github_golang_groupcache",
    commit = "24b0969c4cb722950103eed87108c8d291a8df00",
    importpath = "github.com/golang/groupcache",
)

go_repository(
    name = "com_github_golang_protobuf",
    commit = "b4deda0973fb4c70b50d226b1af49f3da59f5265",
    importpath = "github.com/golang/protobuf",
)

go_repository(
    name = "com_github_golang_snappy",
    commit = "2e65f85255dbc3072edf28d6b5b8efc472979f5a",
    importpath = "github.com/golang/snappy",
)

go_repository(
    name = "com_github_google_btree",
    commit = "e89373fe6b4a7413d7acd6da1725b83ef713e6e4",
    importpath = "github.com/google/btree",
)

go_repository(
    name = "com_github_google_gofuzz",
    commit = "24818f796faf91cd76ec7bddd72458fbced7a6c1",
    importpath = "github.com/google/gofuzz",
)

go_repository(
    name = "com_github_googleapis_gnostic",
    commit = "7c663266750e7d82587642f65e60bc4083f1f84e",
    importpath = "github.com/googleapis/gnostic",
)

go_repository(
    name = "com_github_gorilla_context",
    commit = "08b5f424b9271eedf6f9f0ce86cb9396ed337a42",
    importpath = "github.com/gorilla/context",
)

go_repository(
    name = "com_github_gorilla_handlers",
    commit = "90663712d74cb411cbef281bc1e08c19d1a76145",
    importpath = "github.com/gorilla/handlers",
)

go_repository(
    name = "com_github_gorilla_mux",
    commit = "e3702bed27f0d39777b0b37b664b6280e8ef8fbf",
    importpath = "github.com/gorilla/mux",
)

go_repository(
    name = "com_github_gorilla_rpc",
    commit = "5c1378103183095acc1b9289ac1475e3bc1e818e",
    importpath = "github.com/gorilla/rpc",
)

go_repository(
    name = "com_github_gorilla_websocket",
    commit = "ea4d1f681babbce9545c9c5f3d5194a789c89f5b",
    importpath = "github.com/gorilla/websocket",
)

go_repository(
    name = "com_github_grpc_ecosystem_go_grpc_prometheus",
    commit = "c225b8c3b01faf2899099b768856a9e916e5087b",
    importpath = "github.com/grpc-ecosystem/go-grpc-prometheus",
)

go_repository(
    name = "com_github_grpc_ecosystem_grpc_gateway",
    commit = "92583770e3f01b09a0d3e9bdf64321d8bebd48f2",
    importpath = "github.com/grpc-ecosystem/grpc-gateway",
)

go_repository(
    name = "com_github_hashicorp_go_cleanhttp",
    commit = "d5fe4b57a186c716b0e00b8c301cbd9b4182694d",
    importpath = "github.com/hashicorp/go-cleanhttp",
)

go_repository(
    name = "com_github_hashicorp_golang_lru",
    commit = "0fb14efe8c47ae851c0034ed7a448854d3d34cf3",
    importpath = "github.com/hashicorp/golang-lru",
)

go_repository(
    name = "com_github_howeyc_gopass",
    commit = "bf9dde6d0d2c004a008c27aaee91170c786f6db8",
    importpath = "github.com/howeyc/gopass",
)

go_repository(
    name = "com_github_imdario_mergo",
    commit = "9316a62528ac99aaecb4e47eadd6dc8aa6533d58",
    importpath = "github.com/imdario/mergo",
)

go_repository(
    name = "com_github_inconshreveable_mousetrap",
    commit = "76626ae9c91c4f2a10f34cad8ce83ea42c93bb75",
    importpath = "github.com/inconshreveable/mousetrap",
)

go_repository(
    name = "com_github_jmespath_go_jmespath",
    commit = "0b12d6b5",
    importpath = "github.com/jmespath/go-jmespath",
)

go_repository(
    name = "com_github_jonboulle_clockwork",
    commit = "2eee05ed794112d45db504eb05aa693efd2b8b09",
    importpath = "github.com/jonboulle/clockwork",
)

go_repository(
    name = "com_github_json_iterator_go",
    commit = "ab8a2e0c74be9d3be70b3184d9acc634935ded82",
    importpath = "github.com/json-iterator/go",
)

go_repository(
    name = "com_github_juju_ratelimit",
    commit = "59fac5042749a5afb9af70e813da1dd5474f0167",
    importpath = "github.com/juju/ratelimit",
)

go_repository(
    name = "com_github_kubernetes_incubator_external_storage",
    commit = "11dd9d7b2915b8c39147db492fe9d00562955318",
    importpath = "github.com/kubernetes-incubator/external-storage",
)

go_repository(
    name = "com_github_mailru_easyjson",
    commit = "3fdea8d05856a0c8df22ed4bc71b3219245e4485",
    importpath = "github.com/mailru/easyjson",
)

go_repository(
    name = "com_github_mattn_go_runewidth",
    commit = "9e777a8366cce605130a531d2cd6363d07ad7317",
    importpath = "github.com/mattn/go-runewidth",
)

go_repository(
    name = "com_github_matttproud_golang_protobuf_extensions",
    commit = "c12348ce28de40eed0136aa2b644d0ee0650e56c",
    importpath = "github.com/matttproud/golang_protobuf_extensions",
)

go_repository(
    name = "com_github_mitchellh_go_homedir",
    commit = "3864e76763d94a6df2f9960b16a20a33da9f9a66",
    importpath = "github.com/mitchellh/go-homedir",
)

go_repository(
    name = "com_github_mitchellh_mapstructure",
    commit = "bb74f1db0675b241733089d5a1faa5dd8b0ef57b",
    importpath = "github.com/mitchellh/mapstructure",
)

go_repository(
    name = "com_github_modern_go_concurrent",
    commit = "bacd9c7ef1dd9b15be4a9909b8ac7a4e313eec94",
    importpath = "github.com/modern-go/concurrent",
)

go_repository(
    name = "com_github_modern_go_reflect2",
    commit = "4b7aa43c6742a2c18fdef89dd197aaae7dac7ccd",
    importpath = "github.com/modern-go/reflect2",
)

go_repository(
    name = "com_github_nu7hatch_gouuid",
    commit = "179d4d0c4d8d407a32af483c2354df1d2c91e6c3",
    importpath = "github.com/nu7hatch/gouuid",
)

go_repository(
    name = "com_github_opentracing_opentracing_go",
    commit = "902ca977fd85455c364050f985eba376b44315f0",
    importpath = "github.com/opentracing/opentracing-go",
)

go_repository(
    name = "com_github_openzipkin_zipkin_go_opentracing",
    commit = "e877b8e9d1069eaedfe4f27e9b6a51cfe20073c9",
    importpath = "github.com/openzipkin/zipkin-go-opentracing",
)

go_repository(
    name = "com_github_pborman_uuid",
    commit = "e790cca94e6cc75c7064b1332e63811d4aae1a53",
    importpath = "github.com/pborman/uuid",
)

go_repository(
    name = "com_github_petar_gollrb",
    commit = "53be0d36a84c2a886ca057d34b6aa4468df9ccb4",
    importpath = "github.com/petar/GoLLRB",
)

go_repository(
    name = "com_github_pierrec_lz4",
    commit = "1958fd8fff7f115e79725b1288e0b878b3e06b00",
    importpath = "github.com/pierrec/lz4",
)

go_repository(
    name = "com_github_prometheus_client_golang",
    commit = "c5b7fccd204277076155f10851dad72b76a49317",
    importpath = "github.com/prometheus/client_golang",
)

go_repository(
    name = "com_github_prometheus_client_model",
    commit = "99fa1f4be8e564e8a6b613da7fa6f46c9edafc6c",
    importpath = "github.com/prometheus/client_model",
)

go_repository(
    name = "com_github_prometheus_common",
    commit = "7600349dcfe1abd18d72d3a1770870d9800a7801",
    importpath = "github.com/prometheus/common",
)

go_repository(
    name = "com_github_prometheus_procfs",
    commit = "40f013a808ec4fa79def444a1a56de4d1727efcb",
    importpath = "github.com/prometheus/procfs",
)

go_repository(
    name = "com_github_puerkitobio_purell",
    commit = "0bcb03f4b4d0a9428594752bd2a3b9aa0a9d4bd4",
    importpath = "github.com/PuerkitoBio/purell",
)

go_repository(
    name = "com_github_puerkitobio_urlesc",
    commit = "de5bf2ad457846296e2031421a34e2568e304e35",
    importpath = "github.com/PuerkitoBio/urlesc",
)

go_repository(
    name = "com_github_rcrowley_go_metrics",
    commit = "e2704e165165ec55d062f5919b4b29494e9fa790",
    importpath = "github.com/rcrowley/go-metrics",
)

go_repository(
    name = "com_github_satori_go_uuid",
    commit = "f58768cc1a7a7e77a3bd49e98cdd21419399b6a3",
    importpath = "github.com/satori/go.uuid",
)

go_repository(
    name = "com_github_shopify_sarama",
    commit = "35324cf48e33d8260e1c7c18854465a904ade249",
    importpath = "github.com/Shopify/sarama",
)

go_repository(
    name = "com_github_sirupsen_logrus",
    commit = "c155da19408a8799da419ed3eeb0cb5db0ad5dbc",
    importpath = "github.com/sirupsen/logrus",
)

go_repository(
    name = "com_github_soheilhy_cmux",
    commit = "e09e9389d85d8492d313d73d1469c029e710623f",
    importpath = "github.com/soheilhy/cmux",
)

go_repository(
    name = "com_github_spf13_cobra",
    commit = "ef82de70bb3f60c65fb8eebacbb2d122ef517385",
    importpath = "github.com/spf13/cobra",
)

go_repository(
    name = "com_github_spf13_pflag",
    commit = "583c0c0531f06d5278b7d917446061adc344b5cd",
    importpath = "github.com/spf13/pflag",
)

go_repository(
    name = "com_github_tmc_grpc_websocket_proxy",
    commit = "830351dc03c6f07d625727d5f993a463babb20e1",
    importpath = "github.com/tmc/grpc-websocket-proxy",
)

go_repository(
    name = "com_github_ugorji_go",
    commit = "b4c50a2b199d93b13dc15e78929cfb23bfdf21ab",
    importpath = "github.com/ugorji/go",
)

go_repository(
    name = "com_github_xiang90_probing",
    commit = "07dd2e8dfe18522e9c447ba95f2fe95262f63bb2",
    importpath = "github.com/xiang90/probing",
)

go_repository(
    name = "in_gopkg_cheggaaa_pb_v1",
    commit = "2af8bbdea9e99e83b3ac400d8f6b6d1b8cbbf338",
    importpath = "gopkg.in/cheggaaa/pb.v1",
)

go_repository(
    name = "in_gopkg_inf_v0",
    commit = "d2d2541c53f18d2a059457998ce2876cc8e67cbf",
    importpath = "gopkg.in/inf.v0",
)

go_repository(
    name = "in_gopkg_mgo_v2",
    commit = "3f83fa5005286a7fe593b055f0d7771a7dce4655",
    importpath = "gopkg.in/mgo.v2",
)

go_repository(
    name = "in_gopkg_yaml_v2",
    commit = "5420a8b6744d3b0345ab293f6fcba19c978f1183",
    importpath = "gopkg.in/yaml.v2",
)

go_repository(
    name = "io_k8s_api",
    commit = "655e4e87cb5a855657ad325d1a615cf870870c9d",
    importpath = "k8s.io/api",
)

go_repository(
    name = "io_k8s_apimachinery",
    commit = "e386b2658ed20923da8cc9250e552f082899a1ee",
    importpath = "k8s.io/apimachinery",
)

go_repository(
    name = "io_k8s_client_go",
    commit = "d92e8497f71b7b4e0494e5bd204b48d34bd6f254",
    importpath = "k8s.io/client-go",
)

go_repository(
    name = "io_k8s_kube_openapi",
    commit = "d83b052f768a50a309c692a9c271da3f3276ff88",
    importpath = "k8s.io/kube-openapi",
)

go_repository(
    name = "io_k8s_kubernetes",
    commit = "91e7b4fd31fcd3d5f436da26c980becec37ceefe",
    importpath = "k8s.io/kubernetes",
)

go_repository(
    name = "org_golang_google_genproto",
    commit = "ff3583edef7de132f219f0efc00e097cabcc0ec0",
    importpath = "google.golang.org/genproto",
)

go_repository(
    name = "org_golang_google_grpc",
    commit = "168a6198bcb0ef175f7dacec0b8691fc141dc9b8",
    importpath = "google.golang.org/grpc",
)

go_repository(
    name = "org_golang_x_crypto",
    commit = "a49355c7e3f8fe157a85be2f77e6e269a0f89602",
    importpath = "golang.org/x/crypto",
)

go_repository(
    name = "org_golang_x_net",
    commit = "4cb1c02c05b0e749b0365f61ae859a8e0cfceed9",
    importpath = "golang.org/x/net",
)

go_repository(
    name = "org_golang_x_sys",
    commit = "7138fd3d9dc8335c567ca206f4333fb75eb05d56",
    importpath = "golang.org/x/sys",
)

go_repository(
    name = "org_golang_x_text",
    commit = "f21a4dfb5e38f5895301dc265a8def02365cc3d0",
    importpath = "golang.org/x/text",
)

go_repository(
    name = "org_golang_x_time",
    commit = "fbb02b2291d28baffd63558aa44b4b56f178d650",
    importpath = "golang.org/x/time",
)
