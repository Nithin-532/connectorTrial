import { getSklSymbol } from "../../util/config";
import { Logger } from "../../util/logging";
import { ConnectorConfiguration, ConnectorGroup, PublicExchangeConnector, Serializable, SklEvent, TopOfBook, Trade } from "../../types";
import { CryptoComTradeSide, getExchangeSymbol, tradeSideMap } from "./crypto-spot";
import WebSocket from "ws";

const logger = Logger.getInstance('crypto-com-spot-public-connector');

export interface CryptoComDepth {
    bids: [string, string, string][],
    asks: [string, string, string][]
}

interface CryptoComEvent {
    id: number,
    method: string,
    code: number,
    result?: any
}

interface CryptoComTrade {
    trade_id: string,
    product_id: string,
    p: string,
    q: string,
    s: CryptoComTradeSide,
    t: string
}

interface Ticker {
    symbol: string,
    connectorType: string,
    event: string,
    lastPrice: number,
    timestamp: number,
}

export class CrytpoComPublicConnector implements PublicExchangeConnector {
    public publicWebSocketAddress = 'wss://stream.crypto.com/exchange/v1/market'
    public restUrl = 'https://api.crypto.com/exchange/v1'
    public websocket: any
    private sklSymbol: string
    private exchangeSymbol: string

    //Subscription Management
    private subscribedChannelsCount = 0;
    private EXCEED_MAX_SUBSCRIPTIONS = 400
    private lastSubscriptionTime = 0
    private SUBSCRIPTION_DELAY = 1000

    private reconnectAttempts = 0;
    private MAX_RECONNECT_ATTEMPTS = 10;

    // Defining Constructor and Member variables
    constructor(
        private group: ConnectorGroup,
        private config: ConnectorConfiguration,
        private credential?: Credential // No need of credential for market data
    ) {
        this.exchangeSymbol = getExchangeSymbol(this.group, this.config);
        this.sklSymbol = getSklSymbol(this.group, this.config);
    }

    //Establishing the webSocket connection
    public async connect(onMessage: (messages: Serializable[]) => void): Promise<any> {
        return new Promise(async (resolve, reject) => {
            const url = this.publicWebSocketAddress;
            this.websocket = new WebSocket(url);

            this.websocket.on('open', async () => {
                const channels = [
                    `book.${this.exchangeSymbol}`,
                    `trade.${this.exchangeSymbol}`,
                    `ticker.${this.exchangeSymbol}`,
                ]
                this.subscribeToProducts(channels);

                resolve(true);
            });

            this.websocket.on('message', async (message: any) => {
                const receivedMessage = message.toString();
                const cryptoComEvent: CryptoComEvent = JSON.parse(receivedMessage);

                // Respond back for public/heartbeat
                if (cryptoComEvent.method === 'public/heartbeat') {
                    logger.log("HeartBeat Messages", cryptoComEvent);

                    const reply = {
                        id: cryptoComEvent.id,
                        method: "public/respond-heartbeat"
                    }

                    if (!this.websocket || this.websocket.readyState !== WebSocket.OPEN) {
                        logger.error('WebSocket is not connected');
                        return;
                    }

                    this.websocket.send(JSON.stringify(reply));

                    await this.sleep(this.SUBSCRIPTION_DELAY);
                }

                else if (cryptoComEvent.method === 'unsubscribe') {
                    logger.log("Unsubscribed to data successfully", cryptoComEvent);
                }

                else {
                    this.handleMessage(cryptoComEvent, onMessage);
                }
            });

            this.websocket.on('error', (error: Error) => {
                logger.error(`WebSocket error: ${error.toString()}`);
            });

            this.websocket.on('close', async (code: number, reason: string) => {
                logger.log(`WebSocket closed: ${code} - ${reason}`);
                // Reconnect logic
                if (this.reconnectAttempts < this.MAX_RECONNECT_ATTEMPTS) {
                    this.reconnectAttempts++;
                    await this.sleep(this.SUBSCRIPTION_DELAY);
                    await this.connect(onMessage);
                } else {
                    logger.error('Max reconnection attempts reached');
                }
            });
        })
    }

    private sleep(ms: number) {
        return new Promise(resolve => setTimeout(resolve, ms));
    }

    //Handle incoming messages
    private handleMessage(data: CryptoComEvent, onMessage: (messages: Serializable[]) => void): void {
        try {
            const message = data.result;
            const eventType = this.getEventType(message);

            if (eventType) {
                const serializableMessages: Serializable[] = this.createSerializableEvents(eventType, message);
                if (serializableMessages.length > 0) {
                    onMessage(serializableMessages);
                }
            } else {
                // Log unrecognized messages
            }
        } catch (e) {
            logger.error("Data has no method result", data);
        }
    }

    //Implement event type determination
    private getEventType(message: any): SklEvent | null {
        if (message.channel === 'trade') {
            return 'Trade';
        } else if (message.channel === 'book') {
            return 'TopOfBook';
        } else if (message.channel === 'ticker') {
            return 'Ticker';
        } else if (message.error) {
            logger.error(`Error message received: ${message.error}`);
            return null;
        } else if (message.event === 'subscription') {
            logger.info(`Subscription confirmed: ${JSON.stringify(message)}`);
            return null;
        }
        return null;
    }

    private createTopOfBook(message: any): TopOfBook {
        return {
            symbol: this.exchangeSymbol,
            connectorType: 'Crypto.com',
            event: 'TopOfBook',
            timeStamp: (new Date(message.t)).getTime(),
            askPrice: message.asks[0][0],
            askSize: message.asks[0][1],
            bidPrice: message.bids[0][0],
            bidSize: message.bids[0][1]
        }
    }

    private createTicker(trade: any): Ticker {
        return {
            symbol: this.exchangeSymbol,
            connectorType: 'Crypto.com',
            event: 'Ticker',
            lastPrice: parseFloat(trade.a),
            timestamp: (new Date(trade.t)).getTime(),
        }
    }

    //Create serializable types
    private createSerializableEvents(eventType: SklEvent, message: any): Serializable[] {
        switch (eventType) {
            case 'Trade':
                return [this.createTrade(message.data[0])];
            case 'TopOfBook':
                const topOfOrder: CryptoComDepth = message.data[0];
                return [this.createTopOfBook(topOfOrder)];
            case 'Ticker':
                return [this.createTicker(message.data[0])];
            default:
                return [];
        }
    }

    private createTrade(trade: CryptoComTrade): Trade {
        const tradeSide: string | undefined = trade.s
        return {
            symbol: this.exchangeSymbol,
            connectorType: 'CryptoCom',
            event: 'Trade',
            price: parseFloat(trade.p),
            size: parseFloat(trade.q),
            side: tradeSideMap[tradeSide],
            timestamp: new Date(trade.t).getTime(),
        };
    }

    private async subscribeToProducts(channels: string[]) {
        if (!this.websocket || this.websocket.readyState !== WebSocket.OPEN) {
            logger.error('WebSocket is not connected');
            return;
        }

        if (this.subscribedChannelsCount === this.EXCEED_MAX_SUBSCRIPTIONS) {
            logger.log("Exceeded Max Subscriptions", this.EXCEED_MAX_SUBSCRIPTIONS);
            return;
        }

        const pendingTime = Date.now() - this.lastSubscriptionTime;

        if (pendingTime < 1000) {
            await this.sleep(pendingTime);
        }

        const request = {
            id: Date.now(),
            method: 'subscribe',
            params: {
                channels: channels
            }
        }

        logger.log("Sending message", request);

        this.subscribedChannelsCount++;

        this.websocket.send(JSON.stringify(request));

        this.lastSubscriptionTime = Date.now();
    }

    private async unSubscribeToProducts(channels: string[]) {
        if (!this.websocket || this.websocket.readyState !== WebSocket.OPEN) {
            logger.error('WebSocket is not connected');
            return;
        }
        
        const request = {
            id: Date.now(),
            method: 'unsubscribe',
            params: {
                channels: channels
            }
        }

        this.subscribedChannelsCount--;

        logger.log("Sending message", request);

        await this.websocket.send(JSON.stringify(request));
    }

    //Stop method to unsubscribe from market channels
    public async stop(): Promise<void> {
        try {
            const channels = [
                `book.${this.exchangeSymbol}`,
                `ticker.${this.exchangeSymbol}`,
                `trade.${this.exchangeSymbol}`,
            ];
            await this.unSubscribeToProducts(channels);
        } catch (error) {
            logger.error(`Error during shutdown: ${error}`);
        }
    }
}