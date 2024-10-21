import { ConnectorConfiguration, ConnectorGroup, Side } from "../../types";

export type CryptoComTradeSide = 'BUY' | 'SELL'

export const sideMap: { [key: string]: Side } = {
    'BUY': "Buy",
    "SELL": "Sell"
}

export const tradeSideMap: { [key: string]: Side } = {
    'BUY': "Buy",
    "SELL": "Sell"
}

export const getExchangeSymbol = (symbolGroup: ConnectorGroup, connectorConfig: ConnectorConfiguration):string => {
    if (connectorConfig.quoteAsset === 'PERP') {
        return `${symbolGroup.name}-${connectorConfig.quoteAsset}`
    } else {
        return `${symbolGroup.name}_${connectorConfig.quoteAsset}`
    }
}