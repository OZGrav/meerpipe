#!/usr/bin/env nextflow

params.help = false
if ( params.help ) {
    help = """mwa_search_pipeline.nf: A pipeline that will beamform and perform a pulsar search
             |                        in the entire FOV.
             |Observation selection options:
             |  --list_in   List of observations to process, given in standard format.
             |              These will be crossmatched against PSRDB before job submission.
             |              List format must be:\n* Column 1 - Pulsar name\n* Column 2 - UTC\n* Column 3
             |              Observation PID\nTrailing columns may be left out if needed, but at a minimum the pulsar name must be provided.
             |  --utcs      Start UTC for PSRDB search.
             |              Returns only observations after this UTC timestamp.
             |  --utce      End UTC for PSRDB search.
             |              Returns only observations before this UTC timestamp.
             |  --obs_pid   Project ID for PSRDB search.
             |              Return only observations matching this Project ID.
             |              If not provided, returns all observations.
             |  --pulsar    Pulsar name for PSRDB search.
             |              Returns only observations with this pulsar name.
             |              If not provided, returns all pulsars.
             |  --list_out  Write the list of observations submitted in a processing_jobs.csv file.
             |
             |Processing options:
             |  --use_edge_subints
             |              Use first and last 8 second subints of observation archives
             |              [default: ${params.use_edge_subints}]
             |  --fluxcal   Calibrate flux densities. Should only be done for calibrator observations
             |              [default: ${params.fluxcal}]
             |Other arguments (optional):
             |  --out_dir   Output directory for the candidates files
             |              [default: ${params.out_dir}]
             |  -w          The Nextflow work directory. Delete the directory once the processs
             |              is finished [default: ${workDir}]""".stripMargin()
    println(help)
    exit(0)
}



process obs_list{
    publishDir "./", mode: 'copy', enabled: params.list_out
    beforeScript 'source  /fred/oz005/users/nswainst/code/meerpipe/env_setup.sh; source /home/nswainst/venv/bin/activate'

    input:
    val utcs
    val utce
    val pulsar
    val obs_pid

    output:
    path "processing_jobs.csv"

    """
    #!/usr/bin/env python

    from joins.folded_observations import FoldedObservations
    from graphql_client import GraphQLClient
    from datetime import datetime
    from meerpipe.db_utils import get_pulsarname, utc_psrdb2normal, pid_getshort

    # PSRDB setup
    client = GraphQLClient("${params.psrdb_url}", False)
    foldedobs = FoldedObservations(client, "${params.psrdb_url}", "${params.psrdb_token}")
    foldedobs.get_dicts = True
    foldedobs.set_use_pagination(True)

    # change blanks to nones
    if "${pulsar}" == "":
        pulsar = None
    else:
        pulsar = "${pulsar}"
    if "${obs_pid}" == "":
        obs_pid = None
    else:
        obs_pid = "${obs_pid}"

    # Also convert dates to correct format
    if "${utcs}" == "":
        utcs = None
    else:
        d = datetime.strptime("${utcs}", '%Y-%m-%d-%H:%M:%S')
        utcs = f"{d.date()}T{d.time()}+00:00"
    if "${utce}" == "":
        utce = None
    else:
        d = datetime.strptime("${utce}", '%Y-%m-%d-%H:%M:%S')
        utce = f"{d.date()}T{d.time()}+00:00"

    # Query based on provided parameters
    obs_data = foldedobs.list(
        None,
        pulsar,
        None,
        None,
        None,
        obs_pid,
        None,
        None,
        utcs,
        utce,
    )

    # Output file
    with open("processing_jobs.csv", "w") as out_file:
        for ob in obs_data:
            pulsar_obs = get_pulsarname(ob, client, "${params.psrdb_url}", "${params.psrdb_token}")
            utc_obs = utc_psrdb2normal(ob['node']['processing']['observation']['utcStart'])
            pid_obs = pid_getshort(ob['node']['processing']['observation']['project']['code'])
            out_file.write(f"{pulsar_obs},{utc_obs},{pid_obs}")
    """
}


process psradd {
    beforeScript 'module use /apps/users/pulsar/skylake/modulefiles; module load psrchive/c216582a0'

    input:
    tuple val(pulsar), val(utc), val(obs_pid)

    output:
    tuple val(pulsar), val(utc), val(obs_pid), path("${pulsar}_${utc}.ar")

    """
    if ${params.use_edge_subints}; then
        # Grab all archives
        archives=\$(ls ${params.input_path}/${pulsar}/${utc}/*/*/*.ar)
    else
        # Grab all archives except for the first and last one
        archives=\$(ls ${params.input_path}/${pulsar}/${utc}/*/*/*.ar | head -n-1 | tail -n+2)
    fi

    psradd -E ${params.meertime_ephemerides}/${pulsar}.par -o ${pulsar}_${utc}.ar \${archives}
    """
}


process calibrate {
    beforeScript 'module use /apps/users/pulsar/skylake/modulefiles; module load psrchive/c216582a0'
    publishDir "${params.output_path}/${pulsar}/${utc}/calibrated", mode: 'copy'

    input:
    tuple val(pulsar), val(utc), val(obs_pid)

    output:
    tuple val(pulsar), val(utc), val(obs_pid), path("${pulsar}_${utc}.ar")

    """
    if ${params.use_edge_subints}; then
        # Grab all archives
        archives=\$(ls ${params.input_path}/${pulsar}/${utc}/*/*/*.ar)
    else
        # Grab all archives except for the first and last one
        archives=\$(ls ${params.input_path}/${pulsar}/${utc}/*/*/*.ar | head -n-1 | tail -n+2)
    fi

    # Calibrate the subint archives
    for ar in \$archives; do
        pac -XP -O ./ -e calib  \$ar
    done

    # Combine the calibrate archives
    psradd -E ${params.meertime_ephemerides}/${pulsar}.par -o ${pulsar}_${utc}.ar *calib
    """
}


process meergaurd {
    beforeScript 'module use /apps/users/pulsar/skylake/modulefiles; module load coast_guard/56b8d81'
    publishDir "${params.output_path}/${pulsar}/${utc}/cleaned", mode: 'copy'

    input:
    tuple val(pulsar), val(utc), val(obs_pid), path(archive)

    output:
    tuple val(pulsar), val(utc), val(obs_pid), path(archive), path("${pulsar}_${utc}_zap.ar")

    """
    clean_archive.py -a ${archive} -T ${params.meertime_templates}/${pulsar}.std -o ${pulsar}_${utc}_zap.ar
    """
}


process decimate {
    beforeScript 'module use /apps/users/pulsar/skylake/modulefiles; module load psrchive/c216582a0'
    publishDir "${params.output_path}/${pulsar}/${utc}/decimated", mode: 'copy'

    input:
    tuple val(pulsar), val(utc), val(obs_pid), path(raw_archive), path(cleaned_archive)

    output:
    tuple val(pulsar), val(utc), val(obs_pid), path(raw_archive), path(cleaned_archive), path("${pulsar}_${utc}_zap.*.ar")

    """
    for nsub in ${params.time_subs.join(' ')}; do
        for nchan in ${params.freq_subs.join(' ')}; do
            pam -t \${nsub} -f \${nchan} -S -p -e \${nsub}t\${nchan}ch1p.ar ${cleaned_archive}
            pam -t \${nsub} -f \${nchan} -S -e \${nsub}t\${nchan}ch4p.ar ${cleaned_archive}
        done
    done
    """
}


process fluxcal {
    debug=true
    beforeScript 'source  /fred/oz005/users/nswainst/code/meerpipe/env_setup.sh; source /home/nswainst/venv/bin/activate'
    publishDir "${params.output_path}/${pulsar}/${utc}/decimated", mode: 'copy'

    input:
    tuple val(pulsar), val(utc), val(obs_pid), path(raw_archive), path(cleaned_archive), path(decimated_archives)

    output:
    tuple val(pulsar), val(utc), val(obs_pid), path("${pulsar}_${utc}_zap.*.ar")

    """
    fluxcal -psrname ${pulsar} -obsname ${utc} -obsheader ${params.input_path}/${pulsar}/${utc}/*/*/obs.header -TPfile ${cleaned_archive} -rawfile ${raw_archive} -dec_files ${decimated_archives.join(' ')} -parfile ${params.meertime_ephemerides}/${pulsar}.par
    """
}


workflow {
    // Use PSRDB to work out which obs to process
    if ( params.list_in ) {
        // Check contents of list_in
        obs_data = Channel.fromPath( params.list_in ).splitCsv()
    }
    else {
        obs_list(
            params.utcs,
            params.utce,
            params.pulsar,
            params.obs_pid,
        )
        obs_data = obs_list.out.splitCsv()
    }

    // Combine archives and flux calibrate if option selected
    if ( params.fluxcal ) {
        obs_data_archive = calibrate( obs_data )
    }
    else {
        obs_data_archive = psradd( obs_data )
    }

    // Clean of RFI with MeerGaurd
    meergaurd( obs_data_archive )

    // Decimate into different time and freq chunnks using pam
    decimate( meergaurd.out )

    // Flux calibrate
    fluxcal( decimate.out )

    // Generate TOAs
    // generate_toas(output_dir,config_params,psrname,logger)

    // Summary and clean up jobs
    // generate_summary(output_dir,config_params,psrname,logger)
}


