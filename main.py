import argparse
import concurrent.futures
import subprocess
import time
import json
import os

from azure_manager import AzureManager

azure_manager = AzureManager()

def create_processing_folders(folder_path):
    if not os.path.exists(folder_path):
        os.makedirs(folder_path)
        print(f"Created folder: {folder_path}")

    subfolders = ['configs', 'models', 'processing']
    for subfolder in subfolders:
        subfolder_path = os.path.join(folder_path, subfolder)
        if not os.path.exists(subfolder_path):
            os.makedirs(subfolder_path)
            print(f"Created subfolder: {subfolder_path}")

def update_config_status(config_path, status):
    with open(config_path, 'r') as file:
        config = json.load(file)
        config['status'] = status

    with open(config_path, 'w') as file:
        json.dump(config, file)

def run_blender_rendering(file_path):
    print('Starting rendering process: ', os.path.splitext(os.path.basename(file_path))[0])

    dir = os.path.dirname(os.path.abspath(__file__))
    input_path = os.path.join(dir, file_path)
    output_path = os.path.join(dir, f"results/processing/" + os.path.splitext(os.path.basename(file_path))[0] + ".png")
    
    command = "python ../PixelRendererBlender/blender_rendering.py --input=\"{}\" --output=\"{}\"".format(input_path, output_path)

    start_time = time.time()
    process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
    subprocess_pid = process.pid

    while True:
        output = process.stdout.readline()
        if process.poll() is not None:
            break
        if output:
            print(output.strip())
    # try:
    #     while process.poll() is None:  # Continue monitoring until the subprocess completes
    #         pass
    #         try:
    #             cpu_usage, ram_usage = get_system_metrics(subprocess_pid)
    #             time.sleep(1)  # Add a brief sleep to avoid excessive polling
    #         except (psutil.NoSuchProcess, psutil.ZombieProcess):
    #             # Zombie process detected, ignore the exception and continue
    #             pass
    # except Exception as e:
    #     # Handle other exceptions that may occur during monitoring
    #     print(f"An error occurred during monitoring: {e}")
    
    process.wait()

    end_time = time.time()
    
    elapsed_time_minutes = (end_time - start_time) / 60

    if not os.path.exists(os.path.join(output_path)):
        raise FileNotFoundError(f"The file 'renderer.png' does not exist at path '{output_path}'.")
    else:
        print(f"Rendered png file at path: '{output_path}'.")
    
def process_file(file_path, config):
    print("Processing file: ", file_path)
    print("Processing config: ", config)

    config_path = 'results/configs/' + config["id"] + '.json'
    update_config_status(config_path, status='processing')
    azure_manager.put_media_path(config_path, "configs/models/" + config["id"] + '.json', config["id_token"])

    run_blender_rendering(file_path)

    update_config_status(config_path, status='done')
    azure_manager.put_media_path(config_path, "configs/models/" + config["id"] + '.json', config["id_token"])

    output_path = 'results/processing/' + config["id"] + '.png'
    azure_manager.put_media_path(output_path, "renders/image/" + config["id"] + '.png', config["id_token"])

    os.remove(config_path)
    os.remove(file_path)
    os.remove(output_path)


def process_queue_element():
    config = azure_manager.get_next_config()

    if not config:
        return

    config_name = config['id']

    with open('results/configs/' + config_name + '.json', 'w') as file:
        json.dump(config, file)

    if config:
        local_path = 'results/' + config["model"]
        resp = azure_manager.get_media(local_path, config["model"], config["id_token"])

        process_file(local_path, config)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Rendering Queue App")
    parser.add_argument("--num_process", type=int, default=2, help="Number of simultaneous processes")
    args = parser.parse_args()

    num_simultaneous = args.num_process

    create_processing_folders('results')

    stop_flag = False

    with concurrent.futures.ThreadPoolExecutor(max_workers=num_simultaneous) as executor:
        while True:
            # if check_stop_file_exists():
            #     break

            running_tasks = executor._work_queue.qsize()
            if not stop_flag and running_tasks < num_simultaneous:
                executor.submit(process_queue_element)
            
            time.sleep(10)

    print("Worker ended")