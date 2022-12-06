import re
import json
import os
import csv
from pathlib import Path, PurePath
import pydicom
from pydicom.errors import InvalidDicomError

# todo: a preview function!
# todo: auto run numbers?
# todo: need an exact match option (or just go back to that)

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


class Series:
    def __init__(self, description: str, index: int, chain: dict, strict: bool, json_entries: dict,
                 nonstandard: bool, suffix: str, datatype: str):
        self.description = description
        self.index = index
        self.chain = chain
        self.strict = strict
        self.json_entries = json_entries
        if not nonstandard:
            if datatype not in datatypes:
                raise ValueError('Unknown data type {}'.format(datatype))

            if suffix not in suffixes[datatype]:
                error_string = 'Unknown suffix {} for data type {}\n'.format(suffix, datatype)
                error_string += 'Allowed suffixes are {}'.format(suffixes[datatype])
                raise ValueError(error_string)

        self.datatype = datatype
        self.suffix = suffix

    def get_format_string(self):
        format_string = 'sub-${name}_'
        if self.chain:
            for key, value in [(k, self.chain[k]) for k in entities if k in self.chain]:
                format_string += '{}-{}_'.format(key, value)

        format_string += '{}'.format(self.suffix)

        return format_string


class Study:
    def __init__(self, study_path: Path):
        self.path = study_path
        self.subject = get_subject_name(study_path)
        self.date = get_date(study_path)
        # self.series = os.listdir(study_path)
        self.series_names = [re.match(series_pattern, x.name).group(2) for x in study_path.rglob('Series*')]
        # self.series = [x for x in study_path.rglob('Series*')]
        self.session = None


class Converter:
    def __init__(self, dicom_path, bids_path, autosession=False):
        self.dicom_path = Path(dicom_path)
        self.bids_path = Path(bids_path)
        self.all_studies = None
        self.autosession = autosession
        self.series = []

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

        all_series = {series for study in self.all_studies for series in study.series_names}

        print('\n'.join(sorted(all_series)))
        for series in sorted(all_series):
            for study in self.all_studies:
                count = len([s for s in study.series_names if s.endswith(series)])
                if count > 1:
                    print(f'{count} {series} found in {study.path.name}')

    def generate_scripts(self, script_ext='.sh', script_path=os.getcwd(), slurm=False, additional_commands=None):
        if not self.all_studies:
            print('Nothing to convert')
            return

        # if we don't write the description or participants file, we don't need this here
        # self.bids_path.mkdir(exist_ok=True, parents=True)

        # there will be a command list/slurm file for each series
        for series in self.series:
            if series.index:
                studies_to_convert = [st for st in self.all_studies if
                                      any(series.description in s for s in st.series_names)]
                series_to_convert = []
                for st in studies_to_convert:
                    sorted_series = sorted([s for s in st.series_names if series.description in s],
                                           key=lambda x: get_series_number(x))
                    if len(sorted_series) >= series.index:
                        series_to_convert.append((sorted_series[series.index - 1], st))
                script_name = f'{series.description}-{series.index}'
            else:
                if series.strict:
                    series_to_convert = [(se, st) for st in self.all_studies for se in st.series_names if
                                         series.description == se]
                else:
                    series_to_convert = [(se, st) for st in self.all_studies for se in st.series_names if
                                         series.description in se]
                print(series_to_convert)
                script_name = series.description

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

            command.extend(self.generate_commands(series))

            if not slurm:
                command.append('done')

            script_name = Path(script_path) / (script_name + script_ext)
            print(script_name)
            # todo: write to stdout instead of file as option?
            with open(script_name, 'w') as f:
                for line in command:
                    f.write(line)
                    f.write('\n')

    def add_series(self, series_description, datatype, suffix, chain: dict = None,
                   json_fields=None, nonstandard=False, index=0, strict=True):
        if not chain:
            chain = {}
        if self.autosession and 'ses' not in chain:
            chain['ses'] = '${session}'

        if not json_fields:
            json_fields = {}

        self.series.append(Series(description=series_description,
                                  index=index,
                                  datatype=datatype,
                                  suffix=suffix,
                                  nonstandard=nonstandard,
                                  chain=chain,
                                  strict=strict,
                                  json_entries=json_fields))

    def generate_commands(self, series: Series, dcm2niix_flags=''):

        command = []
        subj_dir = Path('sub-${name}')

        if 'ses' in series.chain:
            output_dir = subj_dir / 'ses-{}'.format(series.chain['ses']) / series.datatype
        elif self.autosession:
            output_dir = subj_dir / 'ses-${session}' / series.datatype
        else:
            output_dir = subj_dir / series.datatype

        format_string = series.get_format_string()
        command.append(f'mkdir --parents "${{bids_path}}/{output_dir}"')
        command.append(
            f'dcmoutput=$(dcm2niix -ba n -l o -o "${{bids_path}}/{output_dir}" -f "{format_string}" {dcm2niix_flags} '
            '${dicom_path}/${input_dir})')
        command.append('echo "${dcmoutput}"')

        if series.json_entries or (series.datatype == 'fmap' and series.suffix == 'auto'):
            command.append('\n# get names of converted files')
            command.append('if grep -q Convert <<< ${dcmoutput} ')
            command.append('  then tmparray=($(echo "${dcmoutput}" | grep Convert ))')
            command.append('  output_files=()')
            command.append('  for ((i=4; i<${#tmparray[@]}; i+=6)); do output_files+=("${tmparray[$i]}"); done')
            command.append('  for output_file in ${output_files[@]}; do')

            if series.json_entries:
                jq_command = '    jq \''
                jq_command += '|'.join([f'.{k} = "{v}"' for k, v in series.json_entries.items()])
                jq_command += '\' ${output_file}.json > ${output_file}.tmp '
                command.append('\n# add fields to json file(s)')
                command.append(jq_command)
                command.append('    mv ${output_file}.tmp ${output_file}.json')

            if series.datatype == 'fmap' and series.suffix == 'auto':
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

        if series.datatype == 'dwi':
            command.append('\n# rename bvecs and bvals files')
            command.append(f'  for x in ${{bids_path}}/{output_dir}/*dwi.bv*')
            command.append('    do mv $x ${x//dwi.}')
            command.append('done')

        return command


# moving into Series class
# class EntityChain:
#     def __init__(self, datatype, suffix, chain: dict = None, nonstandard=False):
#
#         if not nonstandard:
#             if datatype not in datatypes:
#                 raise ValueError('Unknown data type {}'.format(datatype))
#
#             if suffix not in suffixes[datatype]:
#                 error_string = 'Unknown suffix {} for data type {}\n'.format(suffix, datatype)
#                 error_string += 'Allowed suffixes are {}'.format(suffixes[datatype])
#                 raise ValueError(error_string)
#
#         self.datatype = datatype
#         self.suffix = suffix
#         self.strict = strict
#         if chain:
#             self.chain = chain
#         else:
#             self.chain = {}
#
#     def __repr__(self):
#         return_string = 'datatype: {}, suffix: {}, entities: {}'.format(self.datatype, self.suffix, self.chain)
#         return return_string
#
#     def get_format_string(self):
#         format_string = 'sub-${name}_'
#         if self.chain:
#             for key, value in [(k, self.chain[k]) for k in entities if k in self.chain]:
#                 format_string += '{}-{}_'.format(key, value)
#
#         format_string += '{}'.format(self.suffix)
#
#         return format_string
#
#     def __str__(self):
#         return self.get_format_string()
#
#
# # not used yet, may delete
# class BidsEntry:
#     def __init(self, chain: dict, json_fields, nonstandard, strict_match):
#         self.chain = chain
#         self.json_fields = json_fields
#         self.nonstandard = nonstandard
#         self.strict_match = strict_match
#
#
# # not needed, move all into converter
# # explains how to map from series names to bids entries
# class BidsMapping:
#     def __init__(self, autosession=False):
#         self.chain_dict = dict()
#         self.json = dict()
#         self.autosession = autosession
#
#     def add(self, series_description, datatype, suffix, chain: dict = None, json_fields=None, nonstandard=False,
#             index=0, strict=True):
#         if not chain:
#             chain = {}
#         if self.autosession and 'ses' not in chain:
#             chain['ses'] = '${session}'
#         self.chain_dict[(series_description, index)] = EntityChain(datatype=datatype, suffix=suffix,
#                                                                    nonstandard=nonstandard, chain=chain)
#         if json_fields:
#             self.json[(series_description, index)] = json_fields
#         else:
#             self.json[(series_description, index)] = {}
#
#     def __str__(self):
#         return_string = str()
#         for series, index in self.chain_dict:
#             return_string += '{}: {}\n'.format(series, self.chain_dict[(series, index)])
#         return return_string
#
#     def __repr__(self):
#         return_string = str()
#         for series, index in self.chain_dict:
#             return_string += '{}: {}\n'.format(series, self.chain_dict[(series, index)].__repr__())
#         return return_string


def amend_phasediffs(bidsdir):
    phasediff_jsons = Path(bidsdir).rglob('*phasediff*.json')
    for pdfile in phasediff_jsons:
        print(pdfile)
        e1file = pdfile.parent / pdfile.name.replace('phasediff', 'magnitude1')
        if e1file.exists():
            with open(e1file, 'r') as e1f, open(pdfile, 'r+') as pdf:
                pdj = json.load(pdf)
                e1j = json.load(e1f)
                pdj['EchoTime1'] = e1j['EchoTime']
                pdj['EchoTime2'] = pdj['EchoTime']
                pdf.seek(0)
                json.dump(pdj, pdf, indent=4)

        else:
            print(f"can't find {e1file}")
