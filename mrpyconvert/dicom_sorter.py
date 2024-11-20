import argparse

parser = argparse.ArgumentParser()

parser.add_argument('input_dir', help = 'directory containing unsorted dicom files')
parser.add_argument('output_dir', help = 'directory for sorted dicom files')

parser.add_argument('-y', dest = 'sortyear', action = 'store_true',
                    help = 'use the year of the scan as the top level directory')

parser.add_argument('-p', dest = 'preview', action = 'store_true',
                    help = 'preview mode (no movement)')

parser.add_argument('-o', dest = 'overwrite', action = 'store_true',
                    help = 'overwrite existing files')

args = parser.parse_args()
indir = args.input_dir

import os

import pydicom


# Get the list of all files in directory tree at given path
listOfFiles = list()
for (dirpath, dirnames, filenames) in os.walk(indir):
    listOfFiles += [os.path.join(dirpath, file) for file in filenames]


duplicates = False

for file in listOfFiles:
  try:
    ds = pydicom.dcmread(file)
  except:
    print('Unable to read as dicom: ', file)
    continue
  subject = ds.PatientName
  date = ds.StudyDate
  time = ds.StudyTime.split('.')[0]
  series_no = ds.SeriesNumber
  series_desc = ds.SeriesDescription
  study_desc = ds.StudyDescription.split('^')

  if args.sortyear:
    outdir = os.path.join(args.output_dir, date[:4])
  else:
    outdir = args.output_dir
  
  newname = os.path.join(outdir, *study_desc, f'{subject}_{date}_{time}', f'Series_{series_no:02d}_{series_desc}',
                         os.path.basename(file))

  if args.preview:
    print(file, '-->', newname)

  elif not args.overwrite and os.path.exists(newname):
     duplicates = True
  else:
     os.renames(file, newname)


if duplicates:
  print('One or more files already existing and not moved')

