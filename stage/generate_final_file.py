import json
import os
from logs.logger import logger

def generate_final_file_func(stage_dir="stage",stage_file="stage.json"):
    data_array = []
    sink_dir = "sink"
    sink_file = "output.json"

    if not os.path.exists(sink_dir):
        os.makedirs(sink_dir)
        os.path.join(stage_dir,stage_file)
        with open(os.path.join(stage_dir,stage_file)) as f:
            for line in f:
                data_array.append(json.loads(line))
        with open(os.path.join(sink_dir,sink_file), "w") as f:
            json_str = json.dumps(data_array,indent=4)
            f.write(json_str+"\n")
    else:
        with open(os.path.join(stage_dir,stage_file)) as f:
            for line in f:
                data_array.append(json.loads(line))
        with open(os.path.join(sink_dir,sink_file), "w") as f:
            json_str = json.dumps(data_array,indent=4)
            f.write(json_str+"\n")
    logger.info("Final output file generated in {} folder, file name: {}".format(sink_dir,sink_file))
