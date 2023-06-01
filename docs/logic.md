Pipeline Logic
==============

The following sections will explain the logic that the pipeline uses to get the optimum results.


Number of time subints for maximum TOAs
---------------------------------------

To work out the maximum number of TOAs each observation can use we first work out what the signal to noise
of the total signal to noise ratio of the cleaned archive is using `psrstat`:

```bash
SNR=$(psrstat -j FTp -c snr=pdmp -c snr cleaned_data.ar | cut -d '=' -f 2)
```

If we're using multiple frequency subbands we can estimate what the signal to noise will be based on the reduced bandwidth:

```{math}
SNR \propto \sqrt{BW}
```

So for the number of frequency channels the data has been scrunched to ({math}`nchan`) we can estimate the SNR ({math}`SNR_{nchan}`) based on the original SNR ({math}`SNR_O`)

```{math}
SNR_{nchan} = SNR_O \sqrt{ \frac{BW_{nchan}}{BW_O} } = \frac{SNR_O}{ \sqrt{nchan} }
```

With this SNR we can then estimate how many time sub intervals we should split the data into based on

```{math}
SNR \propto \sqrt{t}
```

So if we use the default desired value of SNR ({math}`SNR_D`) of 12 (can be changed with `--tos_sn`)
we can estimate the time sub intervals ({math`nsub`}) with:

```{math}
nsub = \left ( \frac{SNR_{nchan}}{SNR_D} \right ) ^2
```

And since we want to