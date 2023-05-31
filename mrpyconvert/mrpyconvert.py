import re
import json
import os
import pathlib
import itertools
import pydicom

# todo: a preview function!

# BIDS VERSION: 1.8.0
# valid datatype information
datatypes = ['anat', 'func', 'dwi', 'fmap', 'meg', 'eeg', 'ieeg', 'beh']

entities = ['acq', 'ce', 'chunk', 'dir', 'echo', 'flip', 'hemi', 'inv', 'mod', 'mt', 'part', 'proc', 'rec', 'recording',
            'run', 'sample', 'ses', 'space', 'split', 'stain', 'task', 'trc']


# valid suffixes for datatypes
suffixes = dict()
suffixes['anat'] = ['T1w', 'T2w', 'FLAIR', 'T1rho', 'T1map', 'T2map', 'T2starw',
                    'T2starmap', 'PDw', 'PDmap', 'PDT2', 'inplaneT1', 'inplaneT2',
                    'angio', 'defacemask', 'UNIT1']
# auto is not a bids suffix, it's used internally by mrpyconvert
suffixes['fmap'] = ['phasediff', 'phase1', 'phase2', 'magnitude1', 'magnitude2',
                    'magnitude', 'fieldmap', 'epi', 'auto']
suffixes['dwi'] = ['dwi', 'bvec', 'bval', 'sbref']
suffixes['func'] = ['bold', 'cbv', 'sbref', 'events', 'physio', 'stim']
suffixes['perf'] = ['asl', 'm0scan']


def read_dicom(filename):
    if not pathlib.Path(filename).exists() or not pathlib.Path(filename).is_file():
        return False
    try:
        dcm = pydicom.dcmread(filename)
    except pydicom.errors.InvalidDicomError:
        return False
    return dcm


class Entity:
    def __init__(self, description: str, index: int, chain: dict, json_entries: dict,
                 nonstandard: bool, suffix: str, datatype: str, search: str, autorun: bool):
        self.description = description
        self.index = index
        self.chain = chain
        self.search = search
        self.json_entries = json_entries
        self.datatype = datatype
        self.suffix = suffix
        self.nonstandard = nonstandard
        self.autorun = autorun

    def get_format_string(self):
        format_string = 'sub-${name}_'
        if self.chain:
            for key, value in [(k, self.chain[k]) for k in entities if k in self.chain]:
                format_string += '{}-{}_'.format(key, value)

        format_string += '{}'.format(self.suffix)

        return format_string


class Series:
    def __init__(self, series_path: pathlib.Path, subject=None, session=None):
        self.path = series_path
        example_dicom = next((x for x in map(read_dicom, series_path.iterdir()) if x), None)
        if example_dicom:
            self.uid = example_dicom.SeriesInstanceUID
            self.series_number = example_dicom.SeriesNumber
            self.series_description = example_dicom.SeriesDescription
            self.study_uid = example_dicom.StudyInstanceUID
            if subject:
                self.subject = subject
            else:
                self.subject = str(example_dicom.PatientName)
            self.orig_subject = str(example_dicom.PatientName)
            self.date = example_dicom.StudyDate
            self.session = session
            self.image_type = example_dicom.ImageType
            self.has_dicoms = True
        else:
            self.has_dicoms = False


# todo: should autosession be somewhere else?
class Converter:
    def __init__(self, autosession=False):
        self.autosession = autosession
        self.series = []
        self.entities = []

    def add_dicoms(self, dicom_path, subject=None, session=None):
        series_paths = [pathlib.Path(root) for root, dirs, files in os.walk(dicom_path, followlinks=True) if not dirs]
        found_series = [series for s in series_paths if (series := Series(s, subject, session)).has_dicoms]

        if not found_series:
            print('No dicoms found')
            return
        else:
            self.series.extend(found_series)


    def inspect(self, dicom_path=None):
        if not dicom_path:
            all_series = self.series
        else:
            series_paths = [pathlib.Path(root) for root, dirs, files in os.walk(dicom_path, followlinks=True) if
                            not dirs]
            all_series = [series for s in series_paths if (series := Series(s)).has_dicoms]

        if not all_series:
            print(f'No dicoms found in {dicom_path}')
            return

        all_subjects = {x.subject for x in all_series}
        all_studies = {x.study_uid for x in all_series}
        n_subjects = len(all_subjects)
        n_studies = len(all_studies)
        s = 's' if n_subjects != 1 else ''
        ies = 'ies' if n_studies != 1 else 'y'
        print(f'{n_studies} stud{ies} for {n_subjects} subject{s} found.')
        print('Subjects: ' + ' '.join(sorted(all_subjects)))
        descriptions = {s.series_description for s in all_series}
        print('\n'.join(sorted(descriptions)))

        for description in descriptions:
            duplicate_flag = False
            for study in all_studies:
                count = len([s for s in all_series if s.series_description == description and s.study_uid == study])
                if count > 1:
                    duplicate_flag = True
                    continue
            if duplicate_flag:
                print(f'More than one copy of {description} for at least one study')

    def set_names(self, bids_names: dict):
        for series in self.series:
            if series.orig_subject in bids_names:
                series.subject = bids_names[series.orig_subject]

    def generate_scripts(self, bids_path, script_ext='.sh', script_path=os.getcwd(), slurm=False,
                         additional_commands=None, script_prefix=None):

        # assign session numbers to series objects using dates
        if self.autosession:
            all_subjects = {x.subject for x in self.series}
            for subject in all_subjects:
                s_series = [s for s in self.series if s.subject == subject]
                s_series.sort(key = lambda x: (x.date, x.study_uid))
                # get unique values, preserving order
                studies = list(dict.fromkeys(s.study_uid for s in s_series))
                for s in s_series:
                    s.session = studies.index(s.study_uid) + 1

        self.bids_path = pathlib.Path(bids_path)
        if not self.series:
            print('Nothing to convert')
            return

        # if we don't write the description or participants file, we don't need this here
        # self.bids_path.mkdir(exist_ok=True, parents=True)

        # there will be a command list/slurm file for each series
        for entity in self.entities:
            if script_prefix:
                script_name = script_prefix + '-' + entity.description
            else:
                script_name = entity.description

            series_to_consider = [s for s in self.series if re.fullmatch(entity.search, s.series_description)]
            series_to_consider = sorted(series_to_consider, key=lambda x: (x.subject, x.study_uid, x.series_number))

            series_to_convert = []
            if entity.index:
                for k, g in itertools.groupby(series_to_consider, key=lambda x: x.study_uid):
                    if m := next((x for i, x in enumerate(g) if i+1 == entity.index), None): series_to_convert.append(m)
            else:
                series_to_convert = series_to_consider

            runs = []
            if entity.autorun:
                for k, g in itertools.groupby(series_to_consider, key=lambda x: x.study_uid):
                    runs.extend([i + 1 for i, s in enumerate(g)])

            if not series_to_convert:
                print(f'No matching dicoms found for {entity.search}')
                continue

            names = [s.subject for s in series_to_convert]

            # get longest common path
            mpl = min(len(s.path.parents) for s in series_to_convert)
            dicom_path = pathlib.Path().root
            for n in range(0, mpl):
                common_parents = {s.path.parents[n] for s in series_to_convert}
                if len(common_parents) == 1:
                    dicom_path = next(iter(common_parents))
                    break
            # I had purepath here but I don't think it's needed?
            paths = [str(pathlib.Path(s.path).relative_to(dicom_path)) for s in series_to_convert]
            command = ['#!/bin/bash\n']
            if slurm:
                command.append(f'#SBATCH --job-name={script_name}')
                command.append(f'#SBATCH --array=0-{len(names) - 1}')
            if additional_commands:
                for extra_command in additional_commands:
                    command.append(extra_command)

            command.append(f'dicom_path={dicom_path.resolve()}')
            command.append(f'bids_path={self.bids_path.resolve()}')
            command.append('names=({})'.format(' '.join(names)))
            sessions = [s.session for s in series_to_convert]
            if any(sessions):
                command.append('sessions=({})'.format(' '.join([str(s) for s in sessions])))
            if any(runs):
                command.append('runs=({})'.format(' '.join([str(r) for r in runs])))

            command.append('input_dirs=("{}")'.format('" \\\n            "'.join(paths)))
            command.append('\n')

            if slurm:
                command.append('name=${names[$SLURM_ARRAY_TASK_ID]}')
                command.append('input_dir=${input_dirs[$SLURM_ARRAY_TASK_ID]}')
                if any(sessions):
                    command.append('session=${sessions[$SLURM_ARRAY_TASK_ID]}')
                if any(runs):
                    command.append('run=${runs[$SLURM_ARRAY_TASK_ID]}')
            else:
                command.append('for i in "${!names[@]}"; do')
                command.append('  name=${names[$i]}')
                command.append('  input_dir=${input_dirs[$i]}')
                if any(sessions):
                    command.append('  session=${sessions[$i]}')
                if any(runs):
                    command.append('  run=${runs[$i]}')

            command.extend(self.generate_commands(entity))

            if not slurm:
                command.append('done')

            script_name = pathlib.Path(script_path) / (script_name + script_ext)
            print(script_name)
            # todo: write to stdout instead of file as option?
            with open(script_name, 'w') as f:
                for line in command:
                    f.write(line)
                    f.write('\n')

    def set_autosession(self, autosession = True):
        self.autosession = autosession
        for e in self.entities:
            if autosession:
                e.chain['ses'] = '${session}'
            else:
                del e.chain['ses']

    def add_entity(self, name, datatype, suffix, chain: dict = None, search=None,
                   json_entries=None, nonstandard=False, index=None, autorun=False):
        if not chain:
            chain = {}

        if self.autosession and 'ses' not in chain:
            chain['ses'] = '${session}'

        if autorun and 'run' not in chain:
            chain['run'] = '${run}'

        if not json_entries:
            json_entries = {}

        if not search:
            search = name

        if not nonstandard:
            if datatype not in datatypes:
                raise ValueError('Unknown data type {}'.format(datatype))

            if suffix not in suffixes[datatype]:
                error_string = 'Unknown suffix {} for data type {}\n'.format(suffix, datatype)
                error_string += 'Allowed suffixes are {}'.format(suffixes[datatype])
                raise ValueError(error_string)

        self.entities.append(Entity(description=name,
                                    index=index,
                                    datatype=datatype,
                                    suffix=suffix,
                                    nonstandard=nonstandard,
                                    chain=chain,
                                    search=search,
                                    json_entries=json_entries,
                                    autorun=autorun))

    def generate_commands(self, entity: Entity, dcm2niix_flags=''):
        command = []
        subj_dir = pathlib.Path('sub-${name}')

        if 'ses' in entity.chain:
            output_dir = subj_dir / 'ses-{}'.format(entity.chain['ses']) / entity.datatype
        elif self.autosession:
            output_dir = subj_dir / 'ses-${session}' / entity.datatype
        else:
            output_dir = subj_dir / entity.datatype

        format_string = entity.get_format_string()
        command.append(f'  mkdir --parents "${{bids_path}}/{output_dir}"')
        command.append(
            f'  dcmoutput=$(dcm2niix -ba n -l o -o "${{bids_path}}/{output_dir}" -f "{format_string}" {dcm2niix_flags} '
            '${dicom_path}/${input_dir})')
        command.append('  echo "${dcmoutput}"')

        if entity.json_entries or (entity.datatype == 'fmap' and entity.suffix == 'auto'):
            command.append('\n  # get names of converted files')
            command.append('  if grep -q Convert <<< ${dcmoutput}; then ')
            command.append('    tmparray=($(echo "${dcmoutput}" | grep Convert ))')
            command.append('    output_files=()')
            command.append('    for ((i=4; i<${#tmparray[@]}; i+=6)); do output_files+=("${tmparray[$i]}"); done')
            command.append('    for output_file in ${output_files[@]}; do')

            if entity.json_entries:
                jq_command = '      jq \''
                jq_command += '|'.join([f'.{k} = "{v}"' for k, v in entity.json_entries.items()])
                jq_command += '\' ${output_file}.json > ${output_file}.tmp '
                command.append('\n      # add fields to json file(s)')
                command.append(jq_command)
                command.append('      mv ${output_file}.tmp ${output_file}.json')

            if entity.datatype == 'fmap' and entity.suffix == 'auto':
                command.append('\n#   rename fieldmap file(s)')
                command.append('      for filename in ${output_file}*; do')
                command.append('        newname=${output_file}')
                command.append('        if [[ ${filename} =~ "auto_e1" ]]; then')
                command.append('          newname=$(echo ${filename}|sed "s:auto_e1:magnitude1:g"); fi')
                command.append('        if [[ ${filename} =~ "auto_e2" ]]; then')
                command.append('          newname=$(echo ${filename}|sed "s:auto_e2:magnitude2:g"); fi')
                command.append('        if [[ ${filename} =~ "auto_e2_ph" ]]; then')
                command.append('          newname=$(echo ${filename}|sed "s:auto_e2_ph:phasediff:g"); fi')
                command.append('        mv ${filename} ${newname}')
                command.append('      done')

            command.append('  done')
            command.append('fi')

        if entity.datatype == 'dwi':
            command.append('\n#   rename bvecs and bvals files')
            command.append(f'    for x in ${{bids_path}}/{output_dir}/*dwi.bv*')
            command.append('      do mv $x ${x//dwi.}')
            command.append('  done')

        return command


def amend_phasediffs(bids_path):
    phasediff_jsons = pathlib.Path(bids_path).rglob('*phasediff*.json')
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
