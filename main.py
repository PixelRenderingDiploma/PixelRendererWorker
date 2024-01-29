import argparse
import concurrent.futures
import subprocess
import time
import json
import os
import shutil

from azure_manager import AzureManager

azure_manager = AzureManager()

def create_processing_folders(folder_path):
    if not os.path.exists(folder_path):
        os.makedirs(folder_path)
        print(f"Created folder: {folder_path}")

def update_config_status(config_path, status):
    with open(config_path, 'r') as file:
        config = json.load(file)
        config['status'] = status

    with open(config_path, 'w') as file:
        json.dump(config, file)

def run_blender_rendering(file_path, output_folder):
    print('Starting rendering process: ', os.path.splitext(os.path.basename(file_path))[0])

    dir = os.path.dirname(os.path.abspath(__file__))
    input_path = os.path.join(dir, file_path)
    output_path = os.path.join(dir, output_folder)
    
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
    
    process.wait()

    end_time = time.time()
    
    elapsed_time_minutes = (end_time - start_time) / 60

    if not os.listdir(output_path):
        raise FileNotFoundError(f"The rendering folder is empty at path '{output_path}'")
    else:
        print(f"Rendered images at path: '{output_path}'")

def run_video_generation(input_folder, video_path):
    print('Starting composing process: ', os.path.splitext(os.path.basename(input_folder))[0])

    dir = os.path.dirname(os.path.abspath(__file__))
    input_path = os.path.join(dir, input_folder)
    output_path = os.path.join(dir, video_path)
    
    command = "python ../PixelRendererBlender/resize.py --input=\"{}\" --output=\"{}\"".format(input_path, output_path)

    start_time = time.time()
    process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
    subprocess_pid = process.pid

    while True:
        output = process.stdout.readline()
        if process.poll() is not None:
            break
        if output:
            print(output.strip())
    
    process.wait()

    end_time = time.time()
    
    elapsed_time_minutes = (end_time - start_time) / 60

    if not os.path.exists(output_path):
        raise FileNotFoundError(f"The video does not exists at path '{output_path}'")
    else:
        print(f"Rendered video at path: '{output_path}'")
    
def process_directory(working_directory, config):
    print("Processing directory: ", working_directory)

    model_path = working_directory + config["id"] + '.glb'
    config_path = working_directory + config["id"] + '.json'

    rendered_directory = working_directory + "rendered_images/"

    video_folder = working_directory + "video/"
    video_path = video_folder + config["id"] + ".mp4"
    if not os.path.exists(video_folder):
        os.makedirs(video_folder)

    update_config_status(config_path, status='rendering')
    azure_manager.put_media_path(config_path, "configs/models/" + config["id"] + '.json', config["id_token"])

    run_blender_rendering(model_path, rendered_directory)
    update_config_status(config_path, status='composing')
    azure_manager.put_media_path(config_path, "configs/models/" + config["id"] + '.json', config["id_token"])

    run_video_generation(rendered_directory, video_path)
    update_config_status(config_path, status='done')
    azure_manager.put_media_path(config_path, "configs/models/" + config["id"] + '.json', config["id_token"])
    azure_manager.put_media_path(video_path, "renders/videos/" + config["id"] + '.mp4', config["id_token"])

def process_queue_element():
    config = azure_manager.get_next_config()

    if not config:
        return

    config_name = config['id']

    working_folder = "results/" + config_name + "/"
    if not os.path.exists(working_folder):
        os.makedirs(working_folder)

    with open(working_folder + config_name + '.json', 'w') as file:
        json.dump(config, file)

    if config:
        model_path = working_folder + config_name + ".glb"
        resp = azure_manager.get_media(model_path, config["model"], config["id_token"])
        
        process_directory(working_folder, config)
        shutil.rmtree(working_folder)

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