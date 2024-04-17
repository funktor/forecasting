import argparse, os
import driver_helper as dh

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    
    parser.add_argument("-p", "--path", help="Task file path", type=str)
    parser.add_argument("-a", "--app_id", help="Client app id", type=str)
    parser.add_argument("-t", "--tenant_id", help="Client tenant id", type=str)
    parser.add_argument("-s", "--app_secret", help="Client app secret", type=str)
    parser.add_argument("-u", "--blob_storage_url", help="Blob storage container URL", type=str)
    
    args = parser.parse_args()
    
    os.environ['APP_ID'] = args.app_id
    os.environ['TENANT_ID'] = args.tenant_id
    os.environ['APP_SECRET'] = args.app_secret
    
    # Running task specified by path
    dh.run(args.path, 
           args.app_id, 
           args.tenant_id,
           args.app_secret, 
           args.blob_storage_url)
    
    print("Done")