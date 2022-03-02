# This is a Python adaptation from: https://github.com/parsifal9/RFlocalfdr

import sys

import numpy as np
import pandas as pd
import patsy
import scipy
import statsmodels.api as sm


def _ff_fit(zz, df=10):
    """
    Fits the spline

    :param zz: Data to be fitted
    :param df: Degrees of freedom for the spline fit
    :returns:
        x: The Bin breaks
        zh: The histogram information
        f.spline: The fitted spline
        counts: The number of data points per bin
    """
    bre = 120
    lo = min(zz)
    up = max(zz)
    zzz = np.maximum(np.minimum(zz, up), lo)
    breaks = np.linspace(lo, up, num=bre)
    zh = np.histogram(zzz, bins=breaks)  # returns counts and breaks
    density, _ = np.histogram(zzz, bins=breaks, density=True)
    zh = (zh + tuple([density]))
    y = zh[0]
    x = (breaks[1:] + breaks[:-1]) / 2  # midpoints

    transformed_x = patsy.dmatrix(f"cr(x, df={df})", {"x": x}, return_type='dataframe')
    transformed_x = sm.add_constant(transformed_x)  # Makes no difference but added for consistency
    fit1 = sm.GLM(y, transformed_x, family=sm.families.Poisson()).fit()
    f_spline = fit1.predict(transformed_x)
    f_hist, _ = np.histogram(zzz, bins=breaks)

    temp = {
        "x": x,
        "zh": zh,
        "f.spline": f_spline,
        "counts": f_hist
    }
    return temp


def _my_dsn(a, x):
    """
    Density of a skew-normal distribution
    :param a: Array length==3 containing location, scale and skewness (xi, omega, and lambda)
        parameters to compute the density distribution
    :param x: Data for which the function is computed
    :return: Density data of the distribution
    """
    xi, omega, lamb = a[0], a[1], a[2]
    z = (x - xi) / omega
    aa = (1 / (2 * np.pi) ** 0.5) * np.exp(-z ** 2 / 2)
    bb = 1 / 3 * lamb ** 3 * z ** 3
    cc = 3 * lamb ** 2 * z ** 2
    hx = np.sqrt(2 / np.pi) * np.exp(-z ** 2 / 2)

    mask = z < -3 / lamb
    hx[mask] = 0
    mask2 = z < -1 / lamb
    hx[mask2 & ~mask] = ((1 / 8) * aa * (9 * lamb * z + cc + bb + 9))[mask2 & ~mask]
    mask = mask2 | mask
    mask2 = z < 1 / lamb
    hx[mask2 & ~mask] = ((1 / 4) * aa * (3 * lamb * z + bb + 4))[mask2 & ~mask]
    mask = mask2 | mask
    mask2 = z < 3 / lamb
    hx[mask2 & ~mask] = ((1 / 8) * aa * (9 * lamb * z - cc + bb + 7))[mask2 & ~mask]

    return (1 / omega) * hx


def _my_dsn_cost(a, df):
    """
    The cost function between the data and the result of estimated parameters
    :param a: Array length==3 containing location, scale and skewness (xi, omega, and lambda)
        parameters to compute the density distribution
    :param df: Pandas DataFrame with two columns x,y representing the input and output respectively
    :return: The difference between the real data and the estimated value at each point
    """
    return _my_dsn(a, df.x) - df.y




def _fit_to_data_set(df):
    """
    Based on the input data (df), fit_to_data_set finds the local minimum of the my_dsn_cost
    function using the Levenberg-Marquardt algorithm.
    :param df: Pandas DataFrame with two columns x,y representing the input and output respectively
    :return: Array length==3 containing location, scale and skewness (xi, omega, and lambda)
        for the estimated function
    """
    try_counter = 0
    a = None
    b = None
    c = None

    try:
        a = scipy.optimize.least_squares(_my_dsn_cost, x0=[1, 2, 1], args=[df],
                                              method='lm', max_nfev = 400)
    except:
        try_counter += 1

    try:
        b = scipy.optimize.least_squares(_my_dsn_cost, x0=[np.mean(df.x), 2, 1],
                                              args=[df], method='lm', max_nfev = 400)
    except:
        try_counter += 1

    try:
        # Estimate the initial parameters so that the optimizing function works best
        vip_sn_mle = scipy.stats.skewnorm.fit(df.x)
        c = scipy.optimize.least_squares(_my_dsn_cost, x0=vip_sn_mle, args=[df],
                                              method='lm', max_nfev = 400)
    except:
        try_counter += 1

    if a.cost > b.cost and b.cost !=0:
        a = b
    if a.cost > c.cost and c.cost !=0:
        a= c
    return a.x


def _local_fdr(f, x, estimates, FUN=scipy.stats.burr.pdf, p0=1):
    """
    Computes the fdr values.
    :param f: A dictionary containing the fitted splines
    :param x: Array with the importance values
    :param estimates: Array length==3 containing location, scale and skewness (xi, omega, and
        lambda) parameters to compute the density distribution
    :param FUN: Function to compute the probability density. Teh default is the Burr function
    :param p0: Maximum pvalue allowed
    :return: Array of the same length as x of FDR corrected p-values
    """
    f0 = FUN(estimates, x)
    f = f['f.spline']
    f = (sum(f0) * f) / sum(f)
    fdr = np.minimum((p0 * f0) / f, 1)

    return fdr


def _propTrueNullByLocalFDR(p):
    """
    Proportion of true null hypothesis
    :param p: Probabilities of the importances
    :return: Minimum p-value
    """
    n = len(p)
    i = np.array(range(1, n + 1))[::-1]
    p = np.sort(p)[::-1]
    q = np.minimum(n / i * p, 1)
    n1 = n + 1
    return np.sum(i * q) / n / n1 * 2


def _determine_C(f_fit, df, t1, start_at=29):
    """
    Determines the cutoff
    :param f_fit: The fitted spline return from the ff_fit function
    :param df: Pandas DataFrame with two columns x,y representing the input and output respectively
    :param t1: Array length==3 containing location, scale and skewness (xi, omega, and lambda)
        parameters to compute the density distribution
    :param start_at: Integer at which to start the computation
    :return: Array with the cumulative sum at each position
    """
    end_at = 119
    f = f_fit['f.spline']
    x = df.x
    y = df.y
    qq = np.zeros(end_at)*np.inf

    for ii in range(start_at, end_at):
        df2 = df.loc[:ii]

        mm1_df2 = scipy.optimize.leastsq(_my_dsn_cost, x0=t1, args=df2)
        mm1_df2 = mm1_df2[0]

        f0_1 = _my_dsn(mm1_df2, df.x)
        f0_1 = f0_1 + sys.float_info.epsilon
        ppp = np.cumsum(f0_1) * (x[1] - x[0])
        p0 = _propTrueNullByLocalFDR(ppp)
        f0 = (np.sum(f) * f0_1) / np.sum(f0_1)

        qq[ii] = np.cumsum((-f_fit['counts'] * np.log(f0 / (f))) - np.log(p0))[ii]

    return qq


def run_it_importances(imp1):
    """
    Compute the p-values based on the feature importances from the Random Forest
    :param imp1: Random Forest feature importances
    :return: The corrected p-values for the features
    """

    imp1 = imp1 - min(imp1) + sys.float_info.epsilon

    f_fit = _ff_fit(imp1)
    y = f_fit['zh'][2]
    x = f_fit['x']

    df = pd.DataFrame({'x': x, 'y': y})
    initial_estimates = _fit_to_data_set(df)

    C = scipy.stats.skewnorm.ppf(0.95, loc=initial_estimates[0], scale=initial_estimates[1],
                                 a=initial_estimates[2])
    df2 = pd.DataFrame({'x': x[x < C], 'y': y[x < C]})

    try:
        qq = _determine_C(f_fit, df, initial_estimates, start_at=36)
    except:
        qq = None

    cc = None
    if qq is not None:
        # in R the 0 were NA and therefore not factored into the equation
        #qq = np.where(qq == 0, np.repeat(np.inf, len(qq)),qq)
        cc = x[np.argmin(qq)]


    final_estimates = _fit_to_data_set(df2)
    # should we use the cc option and C as a fallback? Usersettable option?

    # determine p0
    ppp = scipy.stats.skewnorm.cdf(imp1, loc=final_estimates[0], scale=final_estimates[1],
                                   a=final_estimates[2])
    p0 = _propTrueNullByLocalFDR(ppp)

    aa = _local_fdr(f_fit, df.x, final_estimates, FUN=_my_dsn, p0=p0)

    mean_aa = np.argmin(np.abs(aa - np.mean(imp1)))
    ww = np.argmin(np.abs(aa[mean_aa:119] - 0.05))
    a1 = imp1[imp1 > df.x[ww + mean_aa]]
    ppp = 1 - scipy.stats.skewnorm.cdf(a1, loc=final_estimates[0], scale=final_estimates[1],
                                       a=final_estimates[2])

    temp = {
        "fdr": aa,
        "x": df.x,
        "estimates": final_estimates,
        "C": C,
        "cc": cc,
        "p0": p0,
        "ppp": ppp}
    return temp
