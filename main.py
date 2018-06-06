import sys, os, re, traceback
from os.path import isfile
from multiprocessing.dummy import Pool, cpu_count
from counter import Counter
from dataAugmentor.rotate import Rotate
from dataAugmentor.fliph import FlipH
from dataAugmentor.flipv import FlipV
from dataAugmentor.zoom import Zoom
from dataAugmentor.blur import Blur
from dataAugmentor.noise import Noise
from dataAugmentor.translate import Translate
from skimage.io import imread, imsave

EXTENSIONS = ['png', 'jpg', 'jpeg', 'bmp']
WORKER_COUNT = max(cpu_count() - 1, 1)
OPERATIONS = [Rotate, FlipH, FlipV, Translate, Noise, Zoom, Blur]

'''
Augmented files will have names matching the regex below, eg

    original__rot90__crop1__flipv.jpg

'''
AUGMENTED_FILE_REGEX = re.compile('^.*(__.+)+\\.[^\\.]+$')
EXTENSION_REGEX = re.compile('|'.join(map(lambda n : '.*\\.' + n + '$', EXTENSIONS)))

thread_pool = None
counter = None

def build_augmented_file_name(original_name, dataAugmentor):
    root, ext = os.path.splitext(original_name)
    result = root
    for op in dataAugmentor:
        result += '__' + op.code
    return result + ext

def work(d, f, op_lists):
    try:
        in_path = os.path.join(d,f)
        for op_list in op_lists:
            out_file_name = build_augmented_file_name(f, op_list)
            if isfile(os.path.join(d,out_file_name)):
                continue
            img = imread(in_path)
            for op in op_list:
                img = op.process(img)
            imsave(os.path.join(d, out_file_name), img)

        counter.processed()
    except:
        traceback.print_exc(file=sys.stdout)

def process(dir, file, op_lists):
    thread_pool.apply_async(work, (dir, file, op_lists))

if __name__ == '__main__':
    if len(sys.argv) < 3:
        print 'Usage: {} <image directory> <operation> (<operation> ...)'.format(sys.argv[0])
        sys.exit(1)

    image_dir = sys.argv[1]
    if not os.path.isdir(image_dir):
        print 'Invalid image directory: {}'.format(image_dir)
        sys.exit(2)

    op_codes = sys.argv[2:]
    op_lists = []
    for op_code_list in op_codes:
        op_list = []
        for op_code in op_code_list.split(','):
            op = None
            for op in OPERATIONS:
                op = op.match_code(op_code)
                if op:
                    op_list.append(op)
                    break

            if not op:
                print 'Unknown operation {}'.format(op_code)
                sys.exit(3)
        op_lists.append(op_list)

    counter = Counter()
    thread_pool = Pool(WORKER_COUNT)
    print 'Thread pool initialised with {} worker{}'.format(WORKER_COUNT, '' if WORKER_COUNT == 1 else 's')

    matches = []
    for dir_info in os.walk(image_dir):
        dir_name, _, file_names = dir_info
        print 'Processing {}...'.format(dir_name)

        for file_name in file_names:
            if EXTENSION_REGEX.match(file_name):
                if AUGMENTED_FILE_REGEX.match(file_name):
                    counter.skipped_augmented()
                else:
                    process(dir_name, file_name, op_lists)
            else:
                counter.skipped_no_match()

    print "Waiting for workers to complete..."
    thread_pool.close()
    thread_pool.join()

    print counter.get()
