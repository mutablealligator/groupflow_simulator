import matplotlib.pyplot as plt
from scipy.stats import truncnorm
from time import sleep

def generate_group_membership_probabilities(num_hosts, mean, std_dev):
    a , b = a, b = (0 - mean) / std_dev, (1 - mean) / std_dev
    midpoint_ab = (b + a) / 2
    scale = 1 / (b - a)
    location = 0.5 - (midpoint_ab * scale)
    print 'Mean: ' + str(mean) + ' StdDev: ' + str(std_dev)
    print 'a: ' + str(a) + ' b: ' + str(b) + ' loc: ' + str(location) + ' scale: ' + str(scale)
    rv = truncnorm(a, b, loc=location, scale=scale)
    return rv.rvs(num_hosts)
    
plt.hist(generate_group_membership_probabilities(500000, 0.2, 0.25), bins=1000)
plt.show()