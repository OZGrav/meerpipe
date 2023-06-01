#!/usr/bin/env nextflow

params.help = false
if ( params.help ) {
    help = """mwa_search_pipeline.nf: A pipeline that will beamform and perform a pulsar search
             |                        in the entire FOV.
             |Observation selection options:
             |  --list_in   List of observations to process, given in a standard format.
             |              Row should include the following: pulsar,utc_obs,project_id,
             |                  band,duration,ephemeris_path,template_path
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
             |Ephemerides and template options:
             |  --ephemerides_dir
             |              Base directory of the ephermerides. Will be used to find a default ephemeris:
             |              \${ephemerides_dir}/\${project}/\${pulsar}.par
             |              [default: ${params.ephemerides_dir}]
             |  --templates_dir
             |              Base directory of the templates. Will be used to find a default templates:
             |              \${templates_dir}/\${project}/\${band}/\${pulsar}.std
             |              [default: ${params.templates_dir}]
             |  --ephemeris Path to the ephemris which will overwrite the default described above.
             |              Recomended to only be used for single observations.
             |  --template  Path to the template which will overwrite the default described above.
             |              Recomended to only be used for single observations.
             |Other arguments (optional):
             |  --out_dir   Output directory for the candidates files
             |              [default: ${params.out_dir}]
             |  -w          The Nextflow work directory. Delete the directory once the processs
             |              is finished [default: ${workDir}]""".stripMargin()
    println(help)
    exit(0)
}



process obs_list{
    label 'meerpipe'
    publishDir "./", mode: 'copy', enabled: params.list_out

    input:
    val utcs
    val utce
    val pulsar
    val obs_pid

    output:
    path "processing_jobs.csv"

    """
    #!/usr/bin/env python

    from glob import glob
    import base64
    from datetime import datetime
    from psrdb.tables.observations import Observations
    from psrdb.graphql_client import GraphQLClient
    from meerpipe.db_utils import get_pulsarname, utc_psrdb2normal, pid_getshort, create_processing
    from meerpipe.archive_utils import get_obsheadinfo, get_rcvr

    # PSRDB setup
    client = GraphQLClient("${params.psrdb_url}", False)
    obs = Observations(client, "${params.psrdb_url}", "${params.psrdb_token}")
    obs.get_dicts = True
    obs.set_use_pagination(True)

    # Query based on provided parameters
    obs_data = obs.list(
        None,
        None,
        "${pulsar}",
        None,
        None,
        None,
        "${obs_pid}",
        None,
        None,
        "${utcs}",
        "${utce}",
    )

    # Output file
    with open("processing_jobs.csv", "w") as out_file:
        for ob in obs_data:
            # Extract data from obs_data
            pulsar_obs = ob['node']['target']['name']
            obs_id = int(base64.b64decode(ob['node']['id']).decode("utf-8").split(":")[1])
            utc_obs = utc_psrdb2normal(ob['node']['utcStart'])
            pid_obs = pid_getshort(ob['node']['project']['code'])

            # Extra data from obs header
            header_data = get_obsheadinfo(glob(f"${params.input_path}/{pulsar_obs}/{utc_obs}/*/*/obs.header")[0])
            band = get_rcvr(header_data)

            # Estimate intergration from number of archives
            nfiles = len(glob(f"${params.input_path}/{pulsar_obs}/{utc_obs}/*/*/*.ar"))

            # Grab ephermis and templates
            if "${params.ephemeris}" == "null":
                ephemeris = f"${params.ephemerides_dir}/{pid_obs}/{pulsar_obs}.par"
            else:
                ephemeris = "${params.ephemeris}"
            if "${params.template}" == "null":
                template = f"${params.templates_dir}/{pid_obs}/{band}/{pulsar_obs}.std"
            else:
                template = "${params.template}"

            # Set job as running
            if "${params.upload}" == "true":
                proc_id = create_processing(
                    obs_id,
                    13, # TODO NOT HARDCODE THIS
                    f"${params.output_path}/{pulsar_obs}/{utc_obs}",
                    # "/fred/oz005/timing_processed/PTA/J1811-2405/2021-11-05-13:22:56/4/1284",
                    client,
                    "${params.psrdb_url}",
                    "${params.psrdb_token}",
                )
            else:
                # No uploading so don't make a processing item
                proc_id = None

            # Write out results
            out_file.write(f"{pulsar_obs},{utc_obs},{pid_obs},{band},{int(nfiles*8)},{proc_id},{ephemeris},{template}\\n")
    """
}


process psradd_calibrate_clean {
    label 'cpu'
    label 'meerpipe'

    publishDir "${params.output_path}/${pulsar}/${utc}/calibrated", mode: 'copy', pattern: "*.ar"
    time   { "${task.attempt * Integer.valueOf(dur) * 0.5} s" }
    memory { "${task.attempt * Integer.valueOf(dur) * 10} MB"}

    input:
    tuple val(pulsar), val(utc), val(obs_pid), val(band), val(dur), val(proc_id), path(ephemeris), path(template)

    output:
    tuple val(pulsar), val(utc), val(obs_pid), val(band), val(dur), val(proc_id), path(ephemeris), path(template), path("${pulsar}_${utc}.ar"), path("${pulsar}_${utc}_zap.ar"), env(SNR)

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
    psradd -E ${ephemeris} -o ${pulsar}_${utc}.ar *calib

    # Update the RM value if available
    rm_cat=\$(python -c "from meerpipe.data_load import RM_CAT;print(RM_CAT)")
    if grep -q "${pulsar}" \${rm_cat}; then
        rm=\$(grep ${pulsar} \${rm_cat} | tr -s ' ' | cut -d ' ' -f 2)
    else
        rm=\$(psrcat -c RM ${pulsar} -X -all | tr -s ' ' | cut -d ' ' -f 1)
    fi
    echo \${rm}
    pam --RM \${rm} -m ${pulsar}_${utc}.ar

    # Clean the archive
    clean_archive.py -a ${pulsar}_${utc}.ar -T ${template} -o ${pulsar}_${utc}_zap.ar

    # Get the signal to noise ratio of the cleaned archive
    SNR=\$(psrstat -j FTp -c snr=pdmp -c snr ${pulsar}_${utc}_zap.ar | cut -d '=' -f 2)
    """
}


process decimate {
    label 'cpu'
    label 'psrchive'

    publishDir "${params.output_path}/${pulsar}/${utc}/decimated", mode: 'copy', pattern: "${pulsar}_${utc}_zap.*.ar"
    time   { "${task.attempt * Integer.valueOf(dur) * 0.5} s" }
    memory { "${task.attempt * Integer.valueOf(dur) * 3} MB"}

    input:
    tuple val(pulsar), val(utc), val(obs_pid), val(band), val(dur), val(proc_id), path(ephemeris), path(template), path(raw_archive), path(cleaned_archive), val(snr)

    output:
    tuple val(pulsar), val(utc), val(obs_pid), val(band), val(dur), val(proc_id), path(ephemeris), path(template), path(raw_archive), path(cleaned_archive), val(snr), path("${pulsar}_${utc}_zap.*.ar")

    """
    for nsub in ${params.time_subs.join(' ')}; do
        input_nsub=\$(vap -c nsub ${cleaned_archive} | tail -n 1 | tr -s ' ' | cut -d ' ' -f 2)
        if [ \$nsub -gt \$input_nsub ]; then
            # Skip if obs not long enough
            continue
        fi
        for nchan in ${params.freq_subs.join(' ')}; do
            echo "Decimate nsub=\${nsub}  nchan=\${nchan} stokes=1"
            pam --setnsub \${nsub} --setnchn \${nchan} -S -p -e \${nsub}t\${nchan}ch1p.temp ${cleaned_archive}
            echo "Delay correct"
            /fred/oz005/users/mkeith/dlyfix/dlyfix -e \${nsub}t\${nchan}ch1p.ar *\${nsub}t\${nchan}ch1p.temp

            echo "Decimate nsub=\${nsub}  nchan=\${nchan} stokes=4"
            pam --setnsub \${nsub} --setnchn \${nchan} -S    -e \${nsub}t\${nchan}ch4p.temp ${cleaned_archive}
            echo "Delay correct"
            /fred/oz005/users/mkeith/dlyfix/dlyfix -e \${nsub}t\${nchan}ch4p.ar *\${nsub}t\${nchan}ch4p.temp
        done
    done
    """
}


process fluxcal {
    label 'cpu'
    label 'meerpipe'

    publishDir "${params.output_path}/${pulsar}/${utc}/fluxcal", mode: 'copy', pattern: "*fluxcal"
    time   { "${task.attempt * Integer.valueOf(dur) * 0.5} s" }
    memory { "${task.attempt * Integer.valueOf(dur) * 10} MB"}

    input:
    tuple val(pulsar), val(utc), val(obs_pid), val(band), val(dur), val(proc_id), path(ephemeris), path(template), path(raw_archive), path(cleaned_archive), val(snr)

    output:
    tuple val(pulsar), val(utc), val(obs_pid), val(band), val(dur), val(proc_id), path(ephemeris), path(template), path("${pulsar}_${utc}.fluxcal"), path("${pulsar}_${utc}_zap.fluxcal"), val(snr) // Replace the archives with flux calced ones

    """
    fluxcal -psrname ${pulsar} -obsname ${utc} -obsheader ${params.input_path}/${pulsar}/${utc}/*/*/obs.header -cleanedfile ${cleaned_archive} -rawfile ${raw_archive} -parfile ${ephemeris}
    """
}


process generate_toas {
    label 'cpu'
    label 'psrchive'

    publishDir "${params.output_path}/${pulsar}/${utc}/timing", mode: 'copy', pattern: "*.{residual,tim,par,std}"
    time   { "${task.attempt * Integer.valueOf(dur) * 1} s" }
    memory { "${task.attempt * Integer.valueOf(dur) * 0.3} MB"}

    input:
    tuple val(pulsar), val(utc), val(obs_pid), val(band), val(dur), val(proc_id), path(ephemeris), path(template), path(raw_archive), path(cleaned_archive), val(snr), path(decimated_archives)

    output:
    tuple val(pulsar), val(utc), val(obs_pid), val(band), val(dur), val(proc_id), path(ephemeris), path(template), path(raw_archive), path(cleaned_archive), val(snr), path(decimated_archives), path("*.tim"), path("*.residual")

    """
    # Loop over each decimated archive
    for ar in ${decimated_archives.join(' ')}; do
        # Grab archive nchan and nsub
        nchan=\$(vap -c nchan \$ar | tail -n 1 | tr -s ' ' | cut -d ' ' -f 2)
        nsub=\$( vap -c nsub  \$ar | tail -n 1 | tr -s ' ' | cut -d ' ' -f 2)
        # Grab template nchan
        tnchan=\$(vap -c nchan ${template} | tail -n 1 | tr -s ' ' | cut -d ' ' -f 2)

        # Use portrait mode if template has more frequency channels
        if [ "\$tnchan" -gt "\$nchan" ]; then
            port="-P"
        else
            port=""
        fi

        echo "Generating TOAs\n----------------------------------"
        pat -jp \$port  -f "tempo2 IPTA" -C "chan rcvr snr length subint" -s ${template} -A FDM \$ar  > \${ar}.tim

        echo "Generating residuals\n----------------------------------"
        tempo2 -nofit -set START 40000 -set FINISH 99999 -output general2 -s "{bat} {post} {err} {freq} BLAH\n" -nobs 1000000 -npsr 1 -select /fred/oz005/users/nswainst/code/meerpipe/default_toa_logic.select -f ${ephemeris} \${ar}.tim > \${ar}.tempo2out && returncode=\$? || returncode=\$?
        if [[ \$returncode -ne 134 && \$returncode -ne 0 ]]; then
            echo "Errorcode: \$returncode. Tempo error other than lack of high S/N data error."
            exit returncode
        elif [[ \$returncode == 134 ]]; then
            echo "Errorcode: \$returncode. No input data due to the logic in /fred/oz005/users/nswainst/code/meerpipe/default_toa_logic.select"
        fi
        cat \${ar}.tempo2out | grep BLAH | awk '{print \$1,\$2,\$3*1e-6,\$4}' > \${ar}.residual
    done
    """
}


process generate_images {
    label 'cpu'
    label 'meerpipe'

    publishDir "${params.output_path}/${pulsar}/${utc}/images", mode: 'copy', pattern: "{c,t,r}*png"
    publishDir "${params.output_path}/${pulsar}/${utc}/scintillation", mode: 'copy', pattern: "*dynspec*"
    time   { "${task.attempt * Integer.valueOf(dur) * 0.5} s" }
    memory { "${task.attempt * Integer.valueOf(dur) * 3} MB"}

    input:
    tuple val(pulsar), val(utc), val(obs_pid), val(band), val(dur), val(proc_id), path(ephemeris), path(template), path(raw_archive), path(cleaned_archive), val(snr), path(decimated_archives), path(toas), path(residuals)

    output:
    tuple val(obs_pid), val(proc_id), path("*.png"), path("*.dat"), path("*dynspec")


    """
    # psrplot images
    for i in "raw ${raw_archive}" "cleaned ${cleaned_archive}"; do
        set -- \$i
        type=\$1
        file=\$2
        # Do the plots for raw file then cleaned file
        psrplot -p flux -jFTDp -jC                -g 1024x768 -c above:l= -c above:c="Stokes I Profile (\${type})"     -D \${type}_profile_fts.png/png \$file
        psrplot -p Scyl -jFTD  -jC                -g 1024x768 -c above:l= -c above:c="Polarisation Profile (\${type})" -D \${type}_profile_ftp.png/png \$file
        psrplot -p freq -jTDp  -jC                -g 1024x768 -c above:l= -c above:c="Phase vs. Frequency (\${type})"  -D \${type}_phase_freq.png/png  \$file
        psrplot -p time -jFDp  -jC                -g 1024x768 -c above:l= -c above:c="Phase vs. Time (\${type})"       -D \${type}_phase_time.png/png  \$file
        psrplot -p b -x -jT -lpol=0,1 -O -c log=1 -g 1024x768 -c above:l= -c above:c="Cleaned bandpass (\${type})"     -D \${type}_bandpass.png/png    \$file
    done

    # Matplotlib images
    generate_images -pid ${obs_pid} -cleanedfile ${cleaned_archive} -rawfile ${raw_archive} -parfile ${ephemeris} -template ${template} -residuals ${residuals} -rcvr ${band}
    """
}


process upload_results {
    debug true
    label 'meerpipe'

    maxForks 1

    input:
    tuple val(obs_pid), val(proc_id), path(dat_files), path(png_files), path(dynspec_files)


    """
    #!/usr/bin/env python

    from glob import glob
    from psrdb.graphql_client import GraphQLClient
    from meerpipe.db_utils import create_pipelineimages
    from meerpipe.initialize import setup_logging

    logger = setup_logging(console=True)
    client = GraphQLClient("${params.psrdb_url}", False)
    pid = '${obs_pid.toLowerCase()}'

    image_data = []
    # grab toa files
    for toa_file in glob("toa*png"):
        # file_loc, file_type, file_rank
        image_data.append( (1, toa_file, f'{pid}.toa-single.hi') )

    # file_rank, file_loc, file_type
    image_data.append( (2,     "raw_profile_ftp.png",       'raw.profile.hi') )
    image_data.append( (2, "cleaned_profile_ftp.png",    f'{pid}.profile-int.hi') )
    image_data.append( (3,     "raw_profile_fts.png",       'raw.profile-pol.hi') )
    image_data.append( (3, "cleaned_profile_fts.png",    f'{pid}.profile-pol.hi') )
    image_data.append( (4,     "raw_phase_time.png",        'raw.phase-time.hi' ) )
    image_data.append( (4, "cleaned_phase_time.png",     f'{pid}.phase-time.hi' ) )
    image_data.append( (5,     "raw_phase_freq.png",        'raw.phase-freq.hi' ) )
    image_data.append( (5, "cleaned_phase_freq.png",     f'{pid}.phase-freq.hi' ) )
    image_data.append( (6,     "raw_bandpass.png",          'raw.bandpass.hi'   ) )
    image_data.append( (6, "cleaned_bandpass.png",       f'{pid}.bandpass.hi'   ) )
    image_data.append( (7,     "raw_SNR_cumulative.png",    'raw.snr-cumul.hi'  ) )
    image_data.append( (7, "cleaned_SNR_cumulative.png", f'{pid}.snr-cumul.hi'  ) )
    image_data.append( (8,     "raw_SNR_single.png",        'raw.snr-single.hi' ) )
    image_data.append( (8, "cleaned_SNR_single.png",     f'{pid}.snr-single.hi' ) )

    # Upload
    # for file_rank, file_loc, file_type in image_data:
    create_pipelineimages(image_data, "${proc_id}", client, "${params.psrdb_url}", "${params.psrdb_token}", logger)
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

    // Combine archives,flux calibrate Clean of RFI with MeerGaurd
    psradd_calibrate_clean( obs_data )

    // Flux calibrate
    fluxcal( psradd_calibrate_clean.out )

    // Decimate into different time and freq chunnks using pam
    decimate( fluxcal.out )

    // Generate TOAs
    generate_toas( decimate.out )

    // Other images using matplotlib and psrplot
    generate_images( generate_toas.out )

    // Upload images and results
    if ( params.upload ) {
        upload_results( generate_images.out )
    }
}


