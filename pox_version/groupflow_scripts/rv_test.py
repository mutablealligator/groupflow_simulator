import matplotlib.pyplot as plt
from scipy.stats import truncnorm
from time import sleep

def generate_group_membership_probabilities(num_hosts, mean, std_dev, avg_group_size = 0):
    a , b = a, b = (0 - mean) / std_dev, (1 - mean) / std_dev
    midpoint_ab = (b + a) / 2
    scale = 1 / (b - a)
    location = 0.5 - (midpoint_ab * scale)
    print 'Mean: ' + str(mean) + ' StdDev: ' + str(std_dev)
    print 'a: ' + str(a) + ' b: ' + str(b) + ' loc: ' + str(location) + ' scale: ' + str(scale)
    rv = truncnorm(a, b, loc=location, scale=scale)
    rvs = rv.rvs(num_hosts)
    if avg_group_size > 0:
        rvs_sum = sum(rvs)
        rvs = [p * (rvs_sum/float(avg_group_size)) for p in rvs]
    print 'Average group size: ' + str(sum(rvs))
    return rvs
    
plt.hist(generate_group_membership_probabilities(500000, 0.2, 0.25, 0), bins=1000)
plt.show()