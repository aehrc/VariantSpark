# This is a Python adaptation from: https://github.com/parsifal9/RFlocalfdr

import sys

import numpy as np
import pandas as pd
import patsy
import scipy
import statsmodels.api as sm


def _ff_fit(zz, df=10, debug_flag=0, temp_dir=''):
    """
    Fits the spline

    :param zz: Data to be fitted
    :param df: Degrees of freedom for the spline fit
    :param debug_flag: Debugging flag
    :param temp_dir: Directory to store the output if debug_flag>0
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

    if debug_flag > 0:
        plot_dir = temp_dir + "/histogram_of_variable_importances.png"
        plt.hist(zzz, bins=breaks)
        plt.plot(x, f_spline, color="red", label='f_spline')
        plt.plot(x, f_hist, color="lime", label='f_hist', linewidth=4)
        plt.xlabel("importance")
        plt.ylabel("counts")
        plt.title("histogram of variable importances")
        plt.legend(loc="upper right")
        plt.savefig(plot_dir)

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




def _fit_to_data_set(df, imp, debug_flag=0, plot_string="", temp_dir=''):
    """
    Based on the input data (df), fit_to_data_set finds the local minimum of the my_dsn_cost
    function using the Levenberg-Marquardt algorithm.
    :param df: Pandas DataFrame with two columns x,y representing the input and output respectively
    :param imp: Data array containing the importances
    :param debug_flag: Debugging flag
    :param plot_string: string to be included to the plot names path
    :param temp_dir: Directory to store the output if debug_flag>0
    :return: Array length==3 containing location, scale and skewness (xi, omega, and lambda)
        for the estimated function
    """
    try_counter = 0
    mm1_df = None
    mm1_df_error = True
    x = df.x
    y = df.y


    if mm1_df_error:
        try:
            mm1_df = scipy.optimize.least_squares(_my_dsn_cost, x0=[1, 2, 1], args=[df],
                                                  method='lm', max_nfev = 400)
            mm1_df_error = False
        except:
            pass
        try_counter += 1

    if debug_flag > 1 and mm1_df_error:
        print(f'try counter {try_counter}, is error: {mm1_df_error} \n')

    if mm1_df_error:
        try:
            mm1_df = scipy.optimize.least_squares(_my_dsn_cost, x0=[np.mean(df.x), 2, 1],
                                                  args=[df], method='lm', max_nfev = 400)
            mm1_df_error = False
        except:
            pass
        try_counter += 1

    if debug_flag > 1 and mm1_df_error:
        print(f'try counter {try_counter}, is error: {mm1_df_error} \n')

    if mm1_df_error:
        # Estimate the initial parameters so that the optimizing function works best
        vip_sn_mle = scipy.stats.skewnorm.fit(df.x)
        try:
            mm1_df = scipy.optimize.least_squares(_my_dsn_cost, x0=vip_sn_mle, args=[df],
                                                  method='lm', max_nfev = 400)
            mm1_df_error = False
        except:
            pass
        try_counter += 1

    mm1_df = mm1_df.x

    if debug_flag > 1 and mm1_df_error:
        print(f'try counter {try_counter}, is error: {mm1_df_error} \n')

    if debug_flag > 0:
        print(f'try = {try_counter}\n')

    if debug_flag > 1:
        plot_dir = temp_dir + "fit_to_data_set_" + plot_string + ".png"
        plt.hist(imp, bins=200)
        plt.plot(df.x, df.y, color="green")

        if try_counter == 3:
            plt.plot(df.x, scipy.stats.skewnorm.pdf(x, loc=vip_sn_mle[0], scale=vip_sn_mle[1],
                                                    a=vip_sn_mle[2]), color='purple')
            plt.plot(df.x, my_dsn(vip_sn_mle, x), color='purple')

        plt.plot(df.x, my_dsn(mm1_df, x), color='red')
        plt.plot(df.x, scipy.stats.skewnorm.pdf(x, loc=mm1_df[0], scale=mm1_df[1], a=mm1_df[2]),
                 color='blue')
        plt.plot(df.x, my_dsn(mm1_df, x), color='blue')

        plt.axvline(x=scipy.stats.skewnorm.pdf(0.5, loc=mm1_df[0], scale=mm1_df[1], a=mm1_df[2]),
                    color='gray')
        plt.axvline(x=scipy.stats.skewnorm.pdf(0.05, loc=mm1_df[0], scale=mm1_df[1], a=mm1_df[2]),
                    color='gray')
        plt.axvline(x=scipy.stats.skewnorm.pdf(0.95, loc=mm1_df[0], scale=mm1_df[1], a=mm1_df[2]),
                    color='gray')

        print(np.sum(my_dsn_cost(mm1_df, df)), "sum(abs(df$y-predict(mm1.df)))", "\n")

        all_labels = [patches.Patch(color="green", label="spline fit"),
                      patches.Patch(color="red", label="fitted f0"),
                      patches.Patch(color="blue", label="Skew-normal at fitted values"),
                      patches.Patch(color="grey", label="quantiles of skew normal")]
        if try_counter == 3:
            all_labels.append(patches.Patch(color="purple", label='initial fitdist fit'))

        plt.legend(handles=all_labels)
        plt.savefig(plot_dir)

    return mm1_df


def _local_fdr(f, x, estimates, FUN=scipy.stats.burr.pdf, p0=1, debug_flag=0, plot_string="",
              temp_dir=''):
    """
    Computes the fdr values.
    :param f: A dictionary containing the fitted splines
    :param x: Array with the importance values
    :param estimates: Array length==3 containing location, scale and skewness (xi, omega, and
        lambda) parameters to compute the density distribution
    :param FUN: Function to compute the probability density. Teh default is the Burr function
    :param p0: Maximum pvalue allowed
    :param debug_flag: Debugging flag
    :param plot_string: string to be included to the plot names path
    :param temp_dir: Directory to store the output if debug_flag>0
    :return: Array of the same length as x of FDR corrected p-values
    """
    f0 = FUN(estimates, x)
    f = f['f.spline']
    f = (sum(f0) * f) / sum(f)
    fdr = np.minimum((p0 * f0) / f, 1)

    if debug_flag > 1:
        plot_dir = temp_dir + "/local_fdr_" + plot_string + ".png"
        plt.scatter(np.arange(len(fdr)), fdr)
        plt.axhline(y=0.2, color='r', ls='--')
        plt.savefig(plot_dir)

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


def _determine_C(f_fit, df, t1, start_at=29, debug_flag=0):
    """
    Determines the cutoff
    :param f_fit: The fitted spline return from the ff_fit function
    :param df: Pandas DataFrame with two columns x,y representing the input and output respectively
    :param t1: Array length==3 containing location, scale and skewness (xi, omega, and lambda)
        parameters to compute the density distribution
    :param start_at: Integer at which to start the computation
    :param debug_flag: Debugging flag
    :return: Array with the cumulative sum at each position
    """
    end_at = 119
    f = f_fit['f.spline']
    x = df.x
    y = df.y
    qq = np.zeros(end_at)*np.inf

    for ii in range(start_at, end_at):
        df2 = df.loc[:ii]

        if debug_flag > 0:
            print("dim(df2)", df2.shape, "\n")

        mm1_df2 = scipy.optimize.leastsq(_my_dsn_cost, x0=t1, args=df2)
        mm1_df2 = mm1_df2[0]

        xi = mm1_df2[0]
        omega = mm1_df2[1]
        lamb = mm1_df2[2]
        f0_1 = _my_dsn(mm1_df2, df.x)
        f0_1 = f0_1 + sys.float_info.epsilon
        ppp = np.cumsum(f0_1) * (x[1] - x[0])
        p0 = _propTrueNullByLocalFDR(ppp)
        f0 = (np.sum(f) * f0_1) / np.sum(f0_1)
        if debug_flag > 0:
            print("p0 = ", p0, "\n")

        qq[ii] = np.cumsum((-f_fit['counts'] * np.log(f0 / (f))) - np.log(p0))[ii]

    return qq


def run_it_importances(imp1, debug_flag=0, temp_dir=''):
    """
    Compute the p-values based on the feature importances from the Random Forest
    :param imp1: Random Forest feature importances
    :param debug_flag: Debugging flag
    :param temp_dir: Directory to store the output if debug_flag>0
    :return: The corrected p-values for the features
    """
    if debug_flag > 0:
        #Importing the plotting libraries only if used
        from matplotlib import patches
        import matplotlib.pyplot as plt
        import seaborn as sns

        temp_dir = './'
        fileConn = open(temp_dir + "/output.txt", 'w')
        fileConn.write("Hello World\n")

    imp1 = imp1 - min(imp1) + sys.float_info.epsilon

    if debug_flag > 0:
        fileConn.write(f"{len(imp1)} length(imp1)\n")
        fileConn.write(f"{min(imp1)} {max(imp1)} min/max(imp1)\n")
        sns.kdeplot(imp1)
        plt.savefig(temp_dir + "/density_importances.png")

    f_fit = _ff_fit(imp1, debug_flag=debug_flag)
    y = f_fit['zh'][2]
    x = f_fit['x']

    if debug_flag > 0:
        plt.plot(x, y)
        plt.savefig(temp_dir + "/density_importances2.png")

    df = pd.DataFrame({'x': x, 'y': y})
    initial_estimates = _fit_to_data_set(df, imp1, debug_flag=debug_flag, plot_string="initial")

    if debug_flag > 0:
        fileConn.write(
            f"initial estimates {initial_estimates[0]}  {initial_estimates[1]} {initial_estimates[2]}\n")

        fileConn.write("we calcualte the fdr using the initial estimates\n")
        aa = _local_fdr(f_fit, df.x, initial_estimates, FUN=_my_dsn, debug_flag=debug_flag,
                       plot_string="initial", temp_dir=temp_dir)
        plt.scatter(x, aa)
        plt.title("fdr using initial estiamtes")
        plt.axhline(0.2, color='r', linestyle='-')
        ww = np.argmin(np.abs(aa[49:119] - 0.2))  # this 50 may need to be tidied up
        tt = sum(imp1 > x[ww])
        # Not really plotted in R > #plt.axvline(x[ww], color = 'r', linestyle = '-')
        plt.savefig(temp_dir + "/initial_estimates.png")
        fileConn.write(f"sum(imp1> x[as.numeric(names(ww))]) {tt}\n")

    C = scipy.stats.skewnorm.ppf(0.95, loc=initial_estimates[0], scale=initial_estimates[1],
                                 a=initial_estimates[2])
    df2 = pd.DataFrame({'x': x[x < C], 'y': y[x < C]})

    if debug_flag > 0:
        print("calculating cc \n")
        if debug_flag == 1:
            fileConn.write(f"calculating C {C}\n")

    try:
        qq = _determine_C(f_fit, df, initial_estimates, start_at=36, debug_flag=debug_flag)
    except:
        qq = None

    if debug_flag > 0:
        fileConn.write(f"{type(qq)} class(determine.C)")

    cc = None
    if qq is not None:
        # in R the 0 were NA and therefore not factored into the equation
        #qq = np.where(qq == 0, np.repeat(np.inf, len(qq)),qq)
        cc = x[np.argmin(qq)]

        if (debug_flag > 0):
            fileConn.write(f"cc= {cc}")
            plt.scatter(x, qq)
            plt.title("determine cc")
            plt.axvline(cc, color='r', linestyle='-')
            plt.savefig(temp_dir + "/determine_cc.png")

    if debug_flag > 1:
        # compare C and cc and the resulting fits

        plt.hist(imp1, bins=100)
        plt.title("compare C and cc and the resulting fits")
        plt.axvline(C, color='r', linestyle='-')
        patchs = []
        patchs.append(patches.Patch(color='red', label='C'))
        if cc is not None:
            plt.axvline(cc, color='purple', linestyle='-')
            patchs.append(patches.Patch(color='purple', label='cc'))

        plt.legend(bbox_to_anchor=(1, 1), handles=patchs)
        plt.savefig(temp_dir + "/compare_C_and_cc_and_the_resulting_fits.png")

        mm1_df2 = _fit_to_data_set(df2, imp1, debug_flag=debug_flag, plot_string="C",
                                  temp_dir=temp_dir)

        if cc is not None:
            df3 = pd.DataFrame({'x': x[x < cc], 'y': y[x < cc]})
            mm1_df3 = _fit_to_data_set(df3, imp1, debug_flag=debug_flag, plot_string="cc",
                                      temp_dir=temp_dir)

        # compare the plots
        plt.hist(imp1, bins=200)
        plt.plot(x, y, color='gray')
        plt.plot(df2.x, df2.y, color='green')
        plt.axvline(C, color='green')
        plt.plot(x, scipy.stats.skewnorm.pdf(x, loc=mm1_df2[0], scale=mm1_df2[1], a=mm1_df2[2]),
                 color='green', label='c')

        if cc is not None:
            plt.plot(df3.x, df3.y, color="blue")
            plt.plot(x, scipy.stats.skewnorm.pdf(x, loc=mm1_df3[0], scale=mm1_df3[1], a=mm1_df3[2]),
                     color='blue', label='cc')

        plt.savefig(temp_dir + "/compare_C_and_cc_and_the_resulting_fits_2.png")

    final_estimates = _fit_to_data_set(df2, imp1, debug_flag=debug_flag, plot_string="final")
    # should we use the cc option and C as a fallback? Usersettable option?

    if debug_flag > 0:
        plt.plot(x, y, color="grey")
        plt.plot(df2.x, df2.y, color="green")
        plt.plot(x, _my_dsn(final_estimates, x))
        plt.savefig(temp_dir + "/fit.to.data.set_df2.png")

    # determine p0
    ppp = scipy.stats.skewnorm.cdf(imp1, loc=final_estimates[0], scale=final_estimates[1],
                                   a=final_estimates[2])
    p0 = _propTrueNullByLocalFDR(ppp)
    if debug_flag == 1:
        fileConn.write(f"{p0} p0")

    aa = _local_fdr(f_fit, df.x, final_estimates, FUN=_my_dsn, p0=p0, debug_flag=debug_flag,
                   plot_string="final")

    mean_aa = np.argmin(np.abs(aa - np.mean(imp1)))
    ww = np.argmin(np.abs(aa[mean_aa:119] - 0.05))
    a1 = imp1[imp1 > df.x[ww + mean_aa]]
    ppp = 1 - scipy.stats.skewnorm.cdf(a1, loc=final_estimates[0], scale=final_estimates[1],
                                       a=final_estimates[2])

    if debug_flag > 0:
        plt.scatter(x, aa)
        plt.axhline(0.2, color='r', linestyle='-')
        # Not really plotted in R plt.axhline(ww, color = 'r', linestyle = '-')
        print(np.sum(imp1 > x[ww]), "sum(imp1> x[as.numeric(names(ww))])", "\n")

        plt.hist(imp1, bins=200)
        plt.axvline(scipy.stats.skewnorm.ppf(0.95, loc=final_estimates[0], scale=final_estimates[1],
                                             a=final_estimates[2]))
        plt.axhline(ww, color='r', linestyle='-')
        plt.savefig(temp_dir + "/cutoff0.95.png")
        print(imp1[imp1 > x[ww]])
        fileConn.close()

    temp = {
        "fdr": aa,
        "x": df.x,
        "estimates": final_estimates,
        "temp.dir": temp_dir,
        "C": C,
        "cc": cc,
        "p0": p0,
        "ppp": ppp}
    return temp
