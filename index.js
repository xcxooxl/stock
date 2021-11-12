const yahooAPI = require('yahoo-finance2')
const parse = require('csv-parse/lib/sync')
const fs = require('fs')
const moment = require('moment')
const alertFile = fs.readFileSync('./alerts.csv');

const records = parse(alertFile, {
    columns: true,
    skip_empty_lines: true
});


const getFullPrice = (priceStr) => {
    let number = parseFloat(priceStr).toFixed(2);
    if (priceStr.includes("K"))
        return (number * 1000).toFixed(0);
    else if (priceStr.includes("M"))
        return (number * 1000000).toFixed(0);
    return number;
}
const dateToString = date => moment(date).format('YYYY-MM-DD');

const getDayIndex = (HLOCV, alert) => {
    if (alert.isCall && alert.Strike > alert.Spot) {
        return HLOCV.findIndex(day => day.high >= alert.Strike);
    } else if (!alert.isCall && alert.Strike <= alert.Spot) {
        return HLOCV.findIndex(day => day.low <= alert.Strike)
    }

    return "N/A"
};


const getTradingDayRange = (HLOCV, alert) => {
    return HLOCV.filter(x => (moment(x.date).startOf('day').unix() > moment(alert.Time).unix() || moment(x.date).isSame(alert.Time, 'day')) && moment(x.date).unix() <= moment(alert.Exp).endOf('day').unix());
};

const findMin = (HLOCV, alert) => {
    const daysInRange = getTradingDayRange(HLOCV, alert);
    return Math.min(...daysInRange.map(x => x.low));
}

const findMax = (HLOCV, alert) => {
    const daysInRange = getTradingDayRange(HLOCV,alert);
    return Math.max(...daysInRange.map(x => x.high));
}

// const findRanges = (HLOCV, alert) => {
//     const daysInRange = HLOCV.filter(x => x.date.getTime() > new Date(alert.Time).getTime() && x.date.getTime() < new Date(alert.Exp).getTime());
//     return '[' + daysInRange.sort((a, b) => new Date(a.Time).getTime() - new Date(b.Time).getTime()).map(x => {
//         return `(${x.low.toFixed(2)} - ${x.high.toFixed(2)})`
//     }) + ']';
// }

const symbols = records.reduce((acc, current) => {
    if (!acc[current.Sym])
        acc[current.Sym] = [];

    acc[current.Sym].push({
        Time: current.Time,
        Sym: current.Sym,
        Strike: getFullPrice(current.Strike),
        Spot: getFullPrice(current.Spot),
        isCall: current["C/P"] === "Call",
        Exp: current["Exp"],
        Orders: current.Orders,
        Volume: getFullPrice(current.Vol),
        Prems: getFullPrice(current.Prems),
        ITM: current.ITM === "1",
    })

    return acc;
}, {});


(async () => {
    const rows = [];
    let progress = 0;
    let errorCounter = 0;
    let symbolsEntries = Object.entries(symbols);
    const totalItems = symbolsEntries.length;
    for (const [sym, alerts] of symbolsEntries) {
        try {
            const period = alerts.sort((a, b) => new Date(a.Time).getTime() - new Date(b.Time).getTime())[0].Time;
            const HLOCVArray = await yahooAPI.default.historical(sym, {
                period1: period,
            })
            for (const alert of alerts) {
                alert.Ignored = false;
                const alertDate = dateToString(new Date(alert.Time));
                const expirationDate = dateToString(new Date(alert.Exp));
                const alertDateIndex = HLOCVArray.findIndex((x) => dateToString(x.date) === alertDate);
                const expirationDateIndex = HLOCVArray.findIndex((x) => dateToString(x.date) === expirationDate);
                const HLOCV = HLOCVArray.slice(alertDateIndex, expirationDateIndex === -1 ? undefined : Math.min(expirationDateIndex+1, HLOCVArray.length))
                let dayIndex = getDayIndex(HLOCV, alert);
                if (dayIndex === "N/A") {
                    alert.Ignored = true;
                    dayIndex = -1;
                }
                alert.Hit = dayIndex !== -1;
                if (alert.Hit) {
                    alert.Expired = moment(HLOCV[dayIndex].date).startOf('day').unix() - moment(expirationDate).unix() > 0;
                } else {
                    alert.Expired = (moment().unix() - moment(expirationDate).unix()) > 0;
                }
                alert.DaysToHit = dayIndex;
                alert.max = findMax(HLOCV, alert);
                alert.min = findMin(HLOCV, alert);
                alert["Call/Put"] = alert.isCall ? "Call" : "Put";
                delete alert.isCall;

                rows.push({
                    Date: alert.Time,
                    ExpirationDate: alert.Exp,
                    Ticker: alert.Sym,
                    Type: alert["Call/Put"],
                    Strike: alert.Strike,
                    Spot: alert.Spot,
                    Orders: alert.Orders,
                    Volume: alert.Volume,
                    Prems: alert.Prems,
                    ITM: alert.ITM ? 'YES' : 'NO',
                    Ignored: alert.Ignored ? 'YES' : 'NO',
                    Hit: alert.Hit ? 'YES' : 'NO',
                    DaysUntilHit: alert.DaysToHit === -1 ? 'N/A' : alert.DaysToHit,
                    Expired: alert.Expired ? 'YES' : 'NO',
                    MaxPrice: alert.max.toFixed(2),
                    MinPrice: alert.min.toFixed(2),
                });

            }
            progress++;
            console.log(`progress: ${(progress / totalItems * 100).toFixed(0)}%, ${progress}/${totalItems} errors: ${errorCounter}`)
        } catch (e) {
            console.log(`error: symbol: ${sym}`)
            errorCounter++;
            progress++;
        }
        await new Promise(resolve => setTimeout(resolve, 800));

        // if (rows.length > 0) break;
    }
    const csvFile = 'results.csv';
    if (fs.existsSync(csvFile))
        fs.unlinkSync(csvFile);
    fs.appendFileSync(csvFile, Object.keys(rows[0]).join(',') + '\n')
    rows.forEach(row => fs.appendFileSync(csvFile, Object.values(row).join(',') + '\n'))
})()
