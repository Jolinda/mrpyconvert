import re
import json
import os
import csv
from pathlib import Path, PurePath
import pydicom
from pydicom.errors import InvalidDicomError

#todo: a preview function!
#todo: auto run numbers?

# valid datatype information
datatypes = ['anat', 'func', 'dwi', 'fmap', 'meg', 'eeg', 'ieeg', 'beh']

entities = ['ses', 'task', 'acq', 'ce', 'rec', 'dir', 'run', 'mod', 'echo', 'recording', 'proc', 'space']

# valid suffixes for datatypes
suffixes = dict()
suffixes['anat'] = ['T1w', 'T2w', 'FLAIR', 'T1rho', 'T1map', 'T2map', 'T2starw',
                    'T2starmap', 'PDw', 'PDmap', 'PDT2', 'inplaneT1', 'inplaneT2',
                    'angio', 'defacemask']
suffixes['fmap'] = ['phasediff', 'phase1', 'phase2', 'magnitude1', 'magnitude2',
                    'magnitude', 'fieldmap', 'epi', 'auto']
suffixes['dwi'] = ['dwi', 'bvec', 'bval']
suffixes['func'] = ['bold', 'cbv', 'phase', 'sbref', 'events', 'physio', 'stim']
suffixes['perf'] = ['asl', 'm0scan']

subject_pattern = re.compile('(.*)_([0-9]{8})(.*)')
series_pattern = re.compile('.*Series_([0-9]*)_(.*)')


# todo: read more info from dicoms instead of from directory names?
# would still assume sorted by subject/series but no assumption about directory names
# could be slower
def is_dicom(filename):
    if not Path(filename).exists() or not Path(filename).is_file():
        return False
    try:
        pydicom.dcmread(filename)
    except pydicom.errors.InvalidDicomError:
        return False
    return True


def get_series_names(directory):
    return set([re.match(series_pattern, x.name).group(2) for x in Path(directory).rglob('Series*')])


def get_subject_name(directory):
    search = re.search(subject_pattern, Path(directory).name)
    if search:
        name = search.group(1)
        return re.sub('[^0-9a-zA-Z]+', '', name)
    else:
        return None


def get_date(directory):
    search = re.search(subject_pattern, Path(directory).name)
    if search:
        return search.group(2)
    else:
        return None


# directory is a string
def get_series_number(directory):
    return int(re.match(series_pattern, directory).group(1))


class Study:
    def __init__(self, study_path):
        self.path = study_path
        self.subject = get_subject_name(study_path)
        self.date = get_date(study_path)
        self.series = os.listdir(study_path)
        self.session = None


# todo: phasediff fieldmaps will need both echo times in json, alTE[0] and [1] in seriesheader
class Converter:
    def __init__(self, dicom_path, bids_path, autosession=False):
        self.dicom_path = Path(dicom_path)
        self.bids_path = Path(bids_path)
        self.all_studies = None
        self.autosession = autosession
        self.bids_dict = BidsDict(autosession)
        study_dirs = [Path(root) for root, dirs, files in os.walk(dicom_path)
                      if re.match(subject_pattern, Path(root).name)]
        if not study_dirs:
            print('No study directories found, dicoms not sorted')
            return

        self.all_studies = [Study(sd) for sd in study_dirs]

        all_subjects = {x.subject for x in self.all_studies}
        if autosession:
            for subject in all_subjects:
                studies = sorted([s for s in self.all_studies if s.subject == subject],
                                 key=lambda x: x.date)
                for i, study in enumerate(studies):
                    self.all_studies[self.all_studies.index(study)].session = i + 1

    def analyze(self):
        all_subjects = {x.subject for x in self.all_studies}
        n_subjects = len(all_subjects)
        n_studies = len(self.all_studies)
        s = 's' if n_subjects != 1 else ''
        ies = 'ies' if n_studies != 1 else 'y'
        print(f'{n_studies} stud{ies} for {n_subjects} subject{s} found.')

        all_series = {re.match(series_pattern, series).group(2)
                      for study in self.all_studies for series in study.series}

        print('\n'.join(sorted(all_series)))
        for series in sorted(all_series):
            for study in self.all_studies:
                count = len([s for s in study.series if s.endswith(series)])
                if count > 1:
                    print(f'{count} {series} found in {study.path.name}')

    # todo: decide what to do about description file, participant file
    def generate_scripts(self, script_ext='.sh', script_path=os.getcwd(), slurm=False, additional_commands=None):
        if not self.all_studies:
            print('Nothing to convert')
            return

        # if we don't write the description or participants file, we don't need this here
        self.bids_path.mkdir(exist_ok=True, parents=True)

        # if description_file:
        #     write_description(self.bids_path)
        #
        # for study in self.all_studies:
        #     if participant_file:
        #         append_participant(study.path, self.bids_path)

        # there will be a command list/slurm file for each series
        for series, index in self.bids_dict.chain_dict:

            if index:
                studies_to_convert = [st for st in self.all_studies if any(series in s for s in st.series)]
                series_to_convert = []
                for st in studies_to_convert:
                    sorted_series = sorted([s for s in st.series if series in s],
                                           key=lambda x: get_series_number(x))
                    if len(sorted_series) >= index:
                        series_to_convert.append((sorted_series[index - 1], st))
                script_name = f'{series}-{index}'
            else:
                series_to_convert = [(se, st) for st in self.all_studies for se in st.series if series in se]
                script_name = series

            names = [st.subject for (se, st) in series_to_convert]
            paths = [str(PurePath(st.path / se).relative_to(self.dicom_path)) for (se, st) in series_to_convert]
            command = ['#!/bin/bash\n']
            if slurm:
                command.append(f'#SBATCH --job-name={script_name}')
                command.append(f'#SBATCH --array=0-{len(names) - 1}')
            if additional_commands:
                for extra_command in additional_commands:
                    command.append(extra_command)

            command.append(f'dicom_path={self.dicom_path}')
            command.append(f'bids_path={self.bids_path}')
            command.append('names=({})'.format(' '.join(names)))
            sessions = [str(st.session) for (se, st) in series_to_convert]
            if any(sessions):
                command.append('sessions=({})'.format(' '.join(sessions)))
            command.append('input_dirs=({})'.format(' \\\n            '.join(paths)))
            command.append('\n')

            if slurm:
                command.append('name=${names[$SLURM_ARRAY_TASK_ID]}')
                command.append('input_dir=${input_dirs[$SLURM_ARRAY_TASK_ID]}')
                if any(sessions):
                    command.append('session=${sessions[$SLURM_ARRAY_TASK_ID]}')
            else:
                command.append('for i in "${!names[@]}"; do')
                command.append('name=${names[$i]}')
                command.append('input_dir=${input_dirs[$i]}')
                if any(sessions):
                    command.append('session=${sessions[$i]}')

            command.extend(self.generate_commands(series, index))

            if not slurm:
                command.append('done')

            script_name = Path(script_path) / (script_name + script_ext)
            print(script_name)
            with open(script_name, 'w') as f:
                for line in command:
                    f.write(line)
                    f.write('\n')

    def generate_commands(self, series_description, index=0, dcm2niix_flags=''):
        if (series_description, index) not in self.bids_dict.chain_dict:
            return []

        command = []
        echain = self.bids_dict.chain_dict[(series_description, index)]
        subj_dir = Path('sub-${name}')

        if 'ses' in echain.chain:
            output_dir = subj_dir / 'ses-{}'.format(echain.chain['ses']) / echain.datatype
        elif self.autosession:
            output_dir = subj_dir / 'ses-${session}' / echain.datatype
        else:
            output_dir = subj_dir / echain.datatype

        format_string = echain.get_format_string()
        command.append(f'mkdir --parents "${{bids_path}}/{output_dir}"')
        command.append(
            f'dcmoutput=$(dcm2niix -ba n -l o -o "${{bids_path}}/{output_dir}" -f "{format_string}" {dcm2niix_flags} '
            '${dicom_path}/${input_dir})')
        command.append('echo "${dcmoutput}"')

        if self.bids_dict.json[(series_description, index)] or (echain.datatype == 'fmap' and echain.suffix == 'auto'):
            command.append('\n# get names of converted files')
            command.append('if grep -q Convert <<< ${dcmoutput} ')
            command.append('  then tmparray=($(echo "${dcmoutput}" | grep Convert ))')
            command.append('  output_files=()')
            command.append('  for ((i=4; i<${#tmparray[@]}; i+=6)); do output_files+=("${tmparray[$i]}"); done')
            command.append('  for output_file in ${output_files[@]}; do')

            if self.bids_dict.json[(series_description, index)]:
                jq_command = '    jq \''
                jq_command += '|'.join([f'.{key} = "{value}"' for key, value in
                                        self.bids_dict.json[(series_description, index)].items()])
                jq_command += '\' ${output_file}.json > ${output_file}.tmp '
                command.append('\n# add fields to json file(s)')
                command.append(jq_command)
                command.append('    mv ${output_file}.tmp ${output_file}.json')

            if echain.datatype == 'fmap' and echain.suffix == 'auto':
                command.append('\n# rename fieldmap file(s)')
                command.append('    for filename in ${output_file}*; do')
                command.append('      newname=${output_file}')
                command.append('      if [[ ${filename} =~ "auto_e1" ]]; then')
                command.append('        newname=$(echo ${filename}|sed "s:auto_e1:magnitude1:g"); fi')
                command.append('      if [[ ${filename} =~ "auto_e2" ]]; then')
                command.append('        newname=$(echo ${filename}|sed "s:auto_e2:magnitude2:g"); fi')
                command.append('      if [[ ${filename} =~ "auto_e2_ph" ]]; then')
                command.append('        newname=$(echo ${filename}|sed "s:auto_e2_ph:phasediff:g"); fi')
                command.append('      mv ${filename} ${newname}')
                command.append('    done')

            command.append('  done')
            command.append('fi')

        if echain.datatype == 'dwi':
            command.append('\n# rename bvecs and bvals files')
            command.append(f'  for x in ${{bids_path}}/{output_dir}/*dwi.bv*')
            command.append('    do mv $x ${x//dwi.}')
            command.append('done')

        return command



class EntityChain:
    def __init__(self, datatype, suffix, chain: dict = None, nonstandard=False):

        if not nonstandard:
            if datatype not in datatypes:
                raise ValueError('Unknown data type {}'.format(datatype))

            if suffix not in suffixes[datatype]:
                error_string = 'Unknown suffix {} for data type {}\n'.format(suffix, datatype)
                error_string += 'Allowed suffixes are {}'.format(suffixes[datatype])
                raise ValueError(error_string)

        self.datatype = datatype
        self.suffix = suffix
        if chain:
            self.chain = chain
        else:
            self.chain = {}

    def __repr__(self):
        return_string = 'datatype: {}, suffix: {}, entities: {}'.format(self.datatype, self.suffix, self.chain)
        return return_string

    def get_format_string(self):
        format_string = 'sub-${name}_'
        if self.chain:
            for key, value in [(k, self.chain[k]) for k in entities if k in self.chain]:
                format_string += '{}-{}_'.format(key, value)

        format_string += '{}'.format(self.suffix)

        return format_string

    def __str__(self):
        return self.get_format_string()


# explains how to map from series names to bids entries
class BidsDict:
    def __init__(self, autosession=False):
        self.chain_dict = dict()
        self.json = dict()
        self.autosession = autosession

    def add(self, series_description, datatype, suffix, chain: dict = None, json=None, nonstandard=False, index=0):
        if not chain:
            chain = {}
        if self.autosession and 'ses' not in chain:
            chain['ses'] = '${session}'
        self.chain_dict[(series_description, index)] = EntityChain(datatype=datatype, suffix=suffix,
                                                                   nonstandard=nonstandard, chain=chain)
        if json:
            self.json[(series_description, index)] = json
        else:
            self.json[(series_description, index)] = {}

    def __str__(self):
        return_string = str()
        for series, index in self.chain_dict:
            return_string += '{}: {}\n'.format(series, self.chain_dict[(series, index)])
        return return_string

    def __repr__(self):
        return_string = str()
        for series, index in self.chain_dict:
            return_string += '{}: {}\n'.format(series, self.chain_dict[(series, index)].__repr__())
        return return_string


# todo: how does this change with multiple sessions?
def append_participant(subjectdir, bidsdir):
    if not os.path.exists(bidsdir):
        os.makedirs(bidsdir)

    name = get_subject_name(subjectdir)
    # check for name in .tsv first
    part_file = os.path.join(bidsdir, 'participants.tsv')

    if os.path.exists(part_file):
        with open(part_file) as tsvfile:
            reader = csv.DictReader(tsvfile, dialect='excel-tab')

            # get the field name
            fieldnames = reader.fieldnames

            subjects = [row['participant_id'] for row in reader]
        # return if this subject is already there
        if 'sub-{}'.format(name) in subjects:
            return

    else:  # create new tsv/json files
        fieldnames = ['participant_id', 'age', 'sex']
        with open(part_file, 'w') as tsvfile:
            writer = csv.DictWriter(tsvfile, fieldnames, dialect='excel-tab',
                                    extrasaction='ignore')
            writer.writeheader()
        json_file = os.path.join(bidsdir, 'participants.json')
        j = {'age': {'Description': 'age of participant', 'Units': 'years'},
             'sex': {'Description': 'sex of participant', 'Levels': {'M': 'male', 'F': 'female', 'O': 'other'}}}
        with open(json_file, 'w') as f:
            json.dump(j, f)

    # get any dicom file
    dcmfile = next(x for x in subjectdir.rglob('*') if is_dicom(x))

    ds = pydicom.dcmread(dcmfile)

    with open(part_file, 'a') as tsvfile:
        writer = csv.DictWriter(tsvfile, fieldnames, dialect='excel-tab',
                                extrasaction='ignore')
        writer.writerow({'participant_id': 'sub-{}'.format(name),
                         'sex': ds.PatientSex, 'age': int(ds.PatientAge[:-1])})
    return
