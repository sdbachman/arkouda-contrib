import argparse
from arkouda_integration.k8s import KubernetesDao, InvocationMethod

def main(crt_file : str, key_file : str, invocation_method : InvocationMethod):
    dao = KubernetesDao()

    if invocation_method == InvocationMethod.GET_POD_IPS:
        print(
            dao.get_pod_ips(namespace=namespace, 
                            app_name=app_name, 
                            pretty_print=True)
        )
    if invocation_method == InvocationMethod.GET_PODS:
        print(
            dao.get_pods(namespace=namespace, 
                         app_name=app_name, 
                         pretty_print=True)
        )
    else:
        logger.error(
            "method {} is not supported from the command line".format(invocation_method)
        )

if __name__ == "__main__":
    arg_parser = argparse.ArgumentParser(description="Arkouda Kubernetes pod clients")

    required = arg_parser.add_argument_group("required arguments")
    optional = arg_parser.add_argument_group("optional arguments")

    required.add_argument(
        "-c", "--crt_file", 
        type=str, 
        required=True,
        help="TLS crt file for connecting to Kubernetes")
    
    required.add_argument(
        "-k", "--key_file", 
        type=str, 
        required=True,
        help="TLS key file for connecting to Kubernetes")
    
    required.add_argument(
        "-i",
        "--invocation_method",
        type=str,
        help="the KubernetesDao method to invoke",
        required=True,
    )

    required.add_argument(
        "-a", "--app_name", 
        type=str, 
        required=True,
        help="Kubernetes app name"
    )
    optional.add_argument(
        "-n",
        "--namespace",
        type=str,
        help="Kubernetes namespace, defaults to default",
        default="default",
        required=False,
    )

    args = arg_parser.parse_args()
    
    crt_file = args.crt_file
    key_file = args.key_file
    invocation_method = args.invocation_method
    app_name = args.app_name
    namespace = args.namespace

    main(crt_file=crt_file,
         key_file=key_file,
         invocation_method=invocation_method,
         app_name=app_name,
         namespace=namespace
         )