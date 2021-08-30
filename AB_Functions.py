#%%


import pandas as pd
import numpy as np

import statsmodels.api as sm
import pandas as pd

def ABnormal(g, Rlag):
    lag = 121 + Rlag
    print(g.name)
    a = pd.DataFrame()
    a = a.append(g)
    a = a.dropna()
    a = a.reset_index(drop=True).reset_index().rename(columns={"index": "Period"})
    a["Period"] = a["Period"].astype(int)
    a["AbnormalReturn"] = np.nan
    a["AbnormalReturn_Market"] = np.nan
    a["AbnormalReturn_WithoutAlpha"] = np.nan
    a["AbnormalReturn_4Factor"] = np.nan

    a["EPeriod"] = np.nan
    a["nEvent"] = np.nan
    a["Beta_CAPM"] = np.nan
    a["Alpha_CAPM"] = np.nan

    a["Beta_CAPMIndustry"] = np.nan
    a["Alpha_CAPMIndustry"] = np.nan
    a["BetaI_CAPMIndustry"] = np.nan

    a["Beta_Market"] = np.nan
    a["Alpha_Market"] = np.nan
    a["Beta_MarketIndustry"] = np.nan
    a["Alpha_MarketIndustry"] = np.nan
    a["BetaI_MarketIndustry"] = np.nan

    a["AbnormalReturn_Industry"] = np.nan
    a["AbnormalReturn_WithoutAlpha_Industry"] = np.nan
    a["AbnormalReturn_MarketIndustry"] = np.nan
    a["AbnormalReturn_MarketModel"] = np.nan
    a["AbnormalReturn_WithoutAlpha_MarketModel"] = np.nan
    a["AbnormalReturn_MarketModel_Industry"] = np.nan
    a["AbnormalReturn_WithoutAlpha_MarketModel_Industry"] = np.nan

    a["betaM_FOUR"] = np.nan
    a["betaS_FOUR"] = np.nan
    a["betaH_FOUR"] = np.nan
    a["betaW_FOUR"] = np.nan
    a["Alpha_FOUR"] = np.nan

    a["AbnormalReturn2"] = np.nan
    a["AbnormalReturn_Market2"] = np.nan
    a["AbnormalReturn_WithoutAlpha2"] = np.nan

    a["Beta_CAPM2"] = np.nan
    a["Alpha_CAPM2"] = np.nan

    a["Beta_CAPMIndustry2"] = np.nan
    a["Alpha_CAPMIndustry2"] = np.nan
    a["BetaI_CAPMIndustry2"] = np.nan

    a["Beta_Market2"] = np.nan
    a["Alpha_Market2"] = np.nan
    a["Beta_MarketIndustry2"] = np.nan
    a["Alpha_MarketIndustry2"] = np.nan
    a["BetaI_MarketIndustry2"] = np.nan

    nEvent = 0
    for i in a[a.Event == a.t]["Period"]:
        nEvent += 1
        tempt = pd.DataFrame()
        tempt = a.loc[(a.Period >= (i - lag)) & (a.Period <= (i + lag))]
        a.loc[(a.Period >= (i - lag)) & (a.Period <= (i + lag)), "EPeriod"] = (
            tempt["Period"] - i
        )
        a.loc[(a.Period >= (i - lag)) & (a.Period <= (i + lag)), "nEvent"] = nEvent

        a.loc[(a.Period >= (i - lag)) & (a.Period <= (i + lag)), "JustRO"] = a[
            a.Period == i
        ]["JustRO"].iloc[0]
        a.loc[(a.Period >= (i - lag)) & (a.Period <= (i + lag)), "JustSaving"] = a[
            a.Period == i
        ]["JustSaving"].iloc[0]
        a.loc[(a.Period >= (i - lag)) & (a.Period <= (i + lag)), "JustPremium"] = a[
            a.Period == i
        ]["JustPremium"].iloc[0]
        a.loc[(a.Period >= (i - lag)) & (a.Period <= (i + lag)), "Hybrid"] = a[
            a.Period == i
        ]["Hybrid"].iloc[0]
        a.loc[(a.Period >= (i - lag)) & (a.Period <= (i + lag)), "Revaluation"] = a[
            a.Period == i
        ]["Revaluation"].iloc[0]

        estimation_window = a.loc[a.EPeriod < -1 * Rlag]
        if len(estimation_window) < 30:
            continue

        # CAPM
        alpha, beta = ols(estimation_window)

        a.loc[
            (a.Period >= (i - lag)) & (a.Period <= (i + lag)), "AbnormalReturn"
        ] = tempt["Return"] - (tempt["RiskFree"] + alpha + beta * tempt["EMR"])

        a.loc[
            (a.Period >= (i - lag)) & (a.Period <= (i + lag)),
            "AbnormalReturn_WithoutAlpha",
        ] = tempt["Return"] - (tempt["RiskFree"] + beta * tempt["EMR"])

        a.loc[
            (a.Period >= (i - lag)) & (a.Period <= (i + lag)), "AbnormalReturn_Market"
        ] = tempt["Return"] - (tempt["RiskFree"] + tempt["EMR"])

        a.loc[(a.Period >= (i - lag)) & (a.Period <= (i + lag)), "Alpha_CAPM"] = alpha

        a.loc[(a.Period >= (i - lag)) & (a.Period <= (i + lag)), "Beta_CAPM"] = beta

        # CAPM + Industry

        alpha, beta, betaI = olsIndustry(estimation_window)
        a.loc[
            (a.Period >= (i - lag)) & (a.Period <= (i + lag)), "AbnormalReturn_Industry"
        ] = tempt["Return"] - (
            tempt["RiskFree"] + alpha + beta * tempt["EMR"] + betaI * tempt["EIR"]
        )

        a.loc[
            (a.Period >= (i - lag)) & (a.Period <= (i + lag)),
            "AbnormalReturn_WithoutAlpha_Industry",
        ] = tempt["Return"] - (
            tempt["RiskFree"] + beta * tempt["EMR"] + betaI * tempt["EIR"]
        )

        a.loc[
            (a.Period >= (i - lag)) & (a.Period <= (i + lag)),
            "AbnormalReturn_MarketIndustry",
        ] = tempt["Return"] - (2 * tempt["RiskFree"] + tempt["EMR"] + tempt["EIR"])

        a.loc[
            (a.Period >= (i - lag)) & (a.Period <= (i + lag)), "Alpha_CAPMIndustry"
        ] = alpha

        a.loc[
            (a.Period >= (i - lag)) & (a.Period <= (i + lag)), "Beta_CAPMIndustry"
        ] = beta
        a.loc[
            (a.Period >= (i - lag)) & (a.Period <= (i + lag)), "BetaI_CAPMIndustry"
        ] = betaI

        # Market Model
        alpha, beta = olsMarket(estimation_window)

        a.loc[
            (a.Period >= (i - lag)) & (a.Period <= (i + lag)),
            "AbnormalReturn_MarketModel",
        ] = tempt["Return"] - (alpha + beta * tempt["Market_return"])

        a.loc[
            (a.Period >= (i - lag)) & (a.Period <= (i + lag)),
            "AbnormalReturn_WithoutAlpha_MarketModel",
        ] = tempt["Return"] - (beta * tempt["Market_return"])

        a.loc[(a.Period >= (i - lag)) & (a.Period <= (i + lag)), "Alpha_Market"] = alpha

        a.loc[(a.Period >= (i - lag)) & (a.Period <= (i + lag)), "Beta_Market"] = beta

        # Market Model + Industry

        alpha, beta, betaI = olsMarketIndustry(estimation_window)
        a.loc[
            (a.Period >= (i - lag)) & (a.Period <= (i + lag)),
            "AbnormalReturn_MarketModel_Industry",
        ] = tempt["Return"] - (
            alpha + beta * tempt["Market_return"] + betaI * tempt["Industry_return"]
        )

        a.loc[
            (a.Period >= (i - lag)) & (a.Period <= (i + lag)),
            "AbnormalReturn_WithoutAlpha_MarketModel_Industry",
        ] = tempt["Return"] - (
            beta * tempt["Market_return"] + betaI * tempt["Industry_return"]
        )

        a.loc[
            (a.Period >= (i - lag)) & (a.Period <= (i + lag)), "Alpha_MarketIndustry"
        ] = alpha

        a.loc[
            (a.Period >= (i - lag)) & (a.Period <= (i + lag)), "Beta_MarketIndustry"
        ] = beta
        a.loc[
            (a.Period >= (i - lag)) & (a.Period <= (i + lag)), "BetaI_MarketIndustry"
        ] = betaI

        # 4Factor
        alpha, betaM, betaS, betaH, betaW = ols4(estimation_window)

        a.loc[
            (a.Period >= (i - lag)) & (a.Period <= (i + lag)), "AbnormalReturn_4Factor"
        ] = tempt["Return"] - (
            tempt["RiskFree"]
            + alpha
            + betaM * tempt["EMR"]
            + betaS * tempt["SMB"]
            + betaH * tempt["HML"]
            + betaW * tempt["Winner_Loser"]
        )

        a.loc[(a.Period >= (i - lag)) & (a.Period <= (i + lag)), "Alpha_FOUR"] = alpha

        a.loc[(a.Period >= (i - lag)) & (a.Period <= (i + lag)), "betaM_FOUR"] = betaM
        a.loc[(a.Period >= (i - lag)) & (a.Period <= (i + lag)), "betaS_FOUR"] = betaS

        a.loc[(a.Period >= (i - lag)) & (a.Period <= (i + lag)), "betaH_FOUR"] = betaH

        a.loc[(a.Period >= (i - lag)) & (a.Period <= (i + lag)), "betaW_FOUR"] = betaW

        #
        #
        #

        estimation_window = a.loc[(a.EPeriod < -1 * Rlag) | (a.EPeriod > 2 * Rlag)]

        # CAPM
        alpha, beta = ols(estimation_window)

        a.loc[
            (a.Period >= (i - lag)) & (a.Period <= (i + lag)), "AbnormalReturn2"
        ] = tempt["Return"] - (tempt["RiskFree"] + alpha + beta * tempt["EMR"])

        a.loc[
            (a.Period >= (i - lag)) & (a.Period <= (i + lag)),
            "AbnormalReturn_WithoutAlpha2",
        ] = tempt["Return"] - (tempt["RiskFree"] + beta * tempt["EMR"])

        a.loc[
            (a.Period >= (i - lag)) & (a.Period <= (i + lag)), "AbnormalReturn_Market2"
        ] = tempt["Return"] - (tempt["RiskFree"] + tempt["EMR"])

        a.loc[(a.Period >= (i - lag)) & (a.Period <= (i + lag)), "Alpha_CAPM2"] = alpha

        a.loc[(a.Period >= (i - lag)) & (a.Period <= (i + lag)), "Beta_CAPM2"] = beta

        # CAPM + Industry

        alpha, beta, betaI = olsIndustry(estimation_window)
        a.loc[
            (a.Period >= (i - lag)) & (a.Period <= (i + lag)),
            "AbnormalReturn_Industry2",
        ] = tempt["Return"] - (
            tempt["RiskFree"] + alpha + beta * tempt["EMR"] + betaI * tempt["EIR"]
        )

        a.loc[
            (a.Period >= (i - lag)) & (a.Period <= (i + lag)),
            "AbnormalReturn_WithoutAlpha_Industry2",
        ] = tempt["Return"] - (
            tempt["RiskFree"] + beta * tempt["EMR"] + betaI * tempt["EIR"]
        )

        a.loc[
            (a.Period >= (i - lag)) & (a.Period <= (i + lag)),
            "AbnormalReturn_MarketIndustry2",
        ] = tempt["Return"] - (2 * tempt["RiskFree"] + tempt["EMR"] + tempt["EIR"])

        a.loc[
            (a.Period >= (i - lag)) & (a.Period <= (i + lag)), "Alpha_CAPMIndustry2"
        ] = alpha

        a.loc[
            (a.Period >= (i - lag)) & (a.Period <= (i + lag)), "Beta_CAPMIndustry2"
        ] = beta
        a.loc[
            (a.Period >= (i - lag)) & (a.Period <= (i + lag)), "BetaI_CAPMIndustry2"
        ] = betaI

        # Market Model
        alpha, beta = olsMarket(estimation_window)

        a.loc[
            (a.Period >= (i - lag)) & (a.Period <= (i + lag)),
            "AbnormalReturn_MarketModel2",
        ] = tempt["Return"] - (alpha + beta * tempt["Market_return"])

        a.loc[
            (a.Period >= (i - lag)) & (a.Period <= (i + lag)),
            "AbnormalReturn_WithoutAlpha_MarketModel2",
        ] = tempt["Return"] - (beta * tempt["Market_return"])

        a.loc[
            (a.Period >= (i - lag)) & (a.Period <= (i + lag)), "Alpha_Market2"
        ] = alpha

        a.loc[(a.Period >= (i - lag)) & (a.Period <= (i + lag)), "Beta_Market2"] = beta

        # Market Model + Industry

        alpha, beta, betaI = olsMarketIndustry(estimation_window)
        a.loc[
            (a.Period >= (i - lag)) & (a.Period <= (i + lag)),
            "AbnormalReturn_MarketModel_Industry2",
        ] = tempt["Return"] - (
            alpha + beta * tempt["Market_return"] + betaI * tempt["Industry_return"]
        )

        a.loc[
            (a.Period >= (i - lag)) & (a.Period <= (i + lag)),
            "AbnormalReturn_WithoutAlpha_MarketMOdel_Industry2",
        ] = tempt["Return"] - (
            beta * tempt["Market_return"] + betaI * tempt["Industry_return"]
        )

        a.loc[
            (a.Period >= (i - lag)) & (a.Period <= (i + lag)), "Alpha_MarketIndustry2"
        ] = alpha

        a.loc[
            (a.Period >= (i - lag)) & (a.Period <= (i + lag)), "Beta_MarketIndustry2"
        ] = beta
        a.loc[
            (a.Period >= (i - lag)) & (a.Period <= (i + lag)), "BetaI_MarketIndustry2"
        ] = betaI

    return a[(~a["EPeriod"].isnull())]


def ols(tempt):
    y, x = "ER", "EMR"
    model = sm.OLS(tempt[y], sm.add_constant(tempt[x])).fit()
    beta = model.params[1]
    alpha = model.params[0]
    return alpha, beta


def olsIndustry(tempt):
    y, x = "ER", ["EMR", "EIR"]
    model = sm.OLS(tempt[y], sm.add_constant(tempt[x])).fit()
    betaI = model.params[2]
    beta = model.params[1]
    alpha = model.params[0]
    return alpha, beta, betaI


def olsMarket(tempt):
    y, x = "Return", "Market_return"
    model = sm.OLS(tempt[y], sm.add_constant(tempt[x])).fit()
    beta = model.params[1]
    alpha = model.params[0]
    return alpha, beta


def olsMarketIndustry(tempt):
    y, x = "Return", ["Market_return", "Industry_return"]
    model = sm.OLS(tempt[y], sm.add_constant(tempt[x])).fit()
    betaI = model.params[2]
    beta = model.params[1]
    alpha = model.params[0]
    return alpha, beta, betaI


def ols4(tempt):
    y, x = "ER", ["EMR", "SMB", "HML", "Winner_Loser"]
    model = sm.OLS(tempt[y], sm.add_constant(tempt[x])).fit()
    betaW = model.params[4]
    betaH = model.params[3]
    betaS = model.params[2]
    betaM = model.params[1]
    alpha = model.params[0]
    return alpha, betaM, betaS, betaH, betaW
