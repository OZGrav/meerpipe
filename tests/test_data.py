from meerpipe.data_load import RM_CAT

def test_catalogue_redundancy():
    pulsars = []
    rms = {}
    with open(RM_CAT, 'r') as f:
        for line in f:
            if line.startswith("#"):
                continue
            pulsar, rm = line.split()
            pulsars.append(pulsar)
            if pulsar in rms.keys():
                rms[pulsar].append(float(rm))
            else:
                rms[pulsar] = [float(rm)]

    for pulsar in rms.keys():
        if len(rms[pulsar]) > 1:
            print(f"{pulsar} has multiple RMs: {rms[pulsar]}")

    assert len(pulsars) == len(set(pulsars))

def test_catalogue_format():
    with open(RM_CAT, 'r') as f:
        for line in f:
            if line.startswith("#"):
                continue
            pulsar, rm = line.split()
            if rm[0] == "-":
                # Remove leading negative signs
                rm = rm[1:]
            if rm[0] == "0" and rm [1] != ".":
                print(f"{pulsar} has a leading zero in RM: {rm}")
                assert False