import argparse
import concurrent.futures
import subprocess
import time
import json
import os
import shutil

from structs import *
from azure_manager import AzureManager
from dataclasses import asdict

azure_manager = AzureManager()

blender_path = os.getenv("BLENDER")

def create_processing_folders(folder_path):
    if not os.path.exists(folder_path):
        os.makedirs(folder_path)
        print(f"Created folder: {folder_path}")

def update_request_status(request_path, status):
    with open(request_path, 'r') as file:
        config = json.load(file)
        config['status'] = status

    with open(request_path, 'w') as file:
        json.dump(config, file)

def run_blender_rendering(file_path, output_folder, settings: RenderingSettings):
    print('Starting rendering process: ', os.path.splitext(os.path.basename(file_path))[0])

    dir = os.path.dirname(os.path.abspath(__file__))
    input_path = os.path.join(dir, file_path)
    output_path = os.path.join(dir, output_folder)
    
    command = [blender_path,
               "-b",
               "--python", "../PixelRendererBlender/blender_rendering.py",
               "--",
               "--input=" + input_path,
               "--output=" + output_path,
               f"--type={settings.type}",
               f"--effect={settings.scene_effect}",
               f"--posteffect={settings.post_effect}",
               f"--target_frame={settings.start_frame}"]

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

def run_upscaling(input_folder, output_file, compose=False):
    print('Starting composing process: ', os.path.splitext(os.path.basename(input_folder))[0])

    dir = os.path.dirname(os.path.abspath(__file__))
    input_path = os.path.join(dir, input_folder)
    output_path = os.path.join(dir, output_file)
    
    command = "python ../PixelRendererBlender/resize.py --input=\"{}\" --output=\"{}\"".format(input_path, output_path)
    if compose == True:
        command += "--compose=True"

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
        print(f"Rendered contenet at path: '{output_path}'")
    
def process_directory(working_directory, request: RenderingRequest):
    print("Processing directory: ", working_directory)

    request_path = working_directory + request.id + '.json'
    model_path = working_directory + request.id_model + '.glb'

    rendered_directory = working_directory + "rendered_images/"

    results_folder = working_directory + "results/"
    result_path = results_folder + request.id
    if request.settings.type == 0:
        result_path += ".png"
    else:
        result_path += ".mp4"

    if not os.path.exists(results_folder):
        os.makedirs(results_folder)

    update_request_status(request_path, status='rendering')
    azure_manager.put_media_path(request_path, "configs/requests/" + request.id + '.json', request.id_token)

    run_blender_rendering(model_path, rendered_directory, request.settings)

    update_request_status(request_path, status='upscaling')
    azure_manager.put_media_path(request_path, "configs/requests/" + request.id + '.json', request.id_token)

    run_upscaling(rendered_directory, result_path, compose=request.settings.type == 1)

    update_request_status(request_path, status='done')
    azure_manager.put_media_path(request_path, "configs/requests/" + request.id + '.json', request.id_token)

    if request.settings.type == 0:
        azure_manager.put_media_path(result_path, "renders/images/" + request.id_model + "/" + request.id + ".png", request.id_token)
    elif request.settings.type == 1:
        azure_manager.put_media_path(result_path, "renders/videos/" + request.id_model + "/" + request.id + ".mp4", request.id_token)

def process_queue_element():
    request = azure_manager.get_next_request()

    if not request:
        return

    working_folder = "results/" + request.id + "/"
    if not os.path.exists(working_folder):
        os.makedirs(working_folder)

    with open(working_folder + request.id + '.json', 'w') as file:
        json.dump(asdict(request), file)

    if request:
        model_path = working_folder + request.id_model + ".glb"
        resp = azure_manager.get_media(model_path, "models/" + request.id_model + ".glb", request.id_token)
        
        process_directory(working_folder, request)

        shutil.rmtree(working_folder)

if __name__ == "__main__":
    load_dotenv()

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