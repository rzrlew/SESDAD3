using System;
using System.Collections;
using System.IO;
using System.Runtime.Remoting;
using System.Runtime.Remoting.Messaging;
using System.Runtime.Remoting.Channels;
using System.Threading;
using System.Net;

namespace SESDAD
{
    public class ClientIPServerSinkProvider :
        IServerChannelSinkProvider
    {
        private IServerChannelSinkProvider _nextProvider = null;

        public ClientIPServerSinkProvider()
        {
        }

        public ClientIPServerSinkProvider(
            IDictionary properties,
            ICollection providerData)
        {
        }

        public IServerChannelSinkProvider Next
        {
            get { return _nextProvider; }
            set { _nextProvider = value; }
        }

        public IServerChannelSink CreateSink(IChannelReceiver channel)
        {
            IServerChannelSink nextSink = null;

            if (_nextProvider != null)
            {
                nextSink = _nextProvider.CreateSink(channel);
            }
            return new ClientIPServerSink(nextSink);
        }

        public void GetChannelData(IChannelDataStore channelData)
        {
        }
    }



    public class ClientIPServerSink :
        BaseChannelObjectWithProperties,
        IServerChannelSink,
        IChannelSinkBase
    {

        private IServerChannelSink _nextSink;

        public ClientIPServerSink(IServerChannelSink next)
        {
            _nextSink = next;
        }

        public IServerChannelSink NextChannelSink
        {
            get { return _nextSink; }
            set { _nextSink = value; }
        }

        public void AsyncProcessResponse(
            IServerResponseChannelSinkStack sinkStack,
            Object state,
            IMessage message,
            ITransportHeaders headers,
            Stream stream)
        {
            IPAddress ip = headers[CommonTransportKeys.IPAddress] as IPAddress;
            string uri = headers[CommonTransportKeys.RequestUri] as string;
            CallContext.SetData("ClientIPAddress", ip);
            CallContext.SetData("ClientURI", uri);
            sinkStack.AsyncProcessResponse(message, headers, stream);
        }

        public Stream GetResponseStream(
            IServerResponseChannelSinkStack sinkStack,
            Object state,
            IMessage message,
            ITransportHeaders headers)
        {

            return null;

        }


        public ServerProcessing ProcessMessage(
            IServerChannelSinkStack sinkStack,
            IMessage requestMsg,
            ITransportHeaders requestHeaders,
            Stream requestStream,
            out IMessage responseMsg,
            out ITransportHeaders responseHeaders,
            out Stream responseStream)
        {
            if (_nextSink != null)
            {
                IPAddress ip = requestHeaders[CommonTransportKeys.IPAddress] as IPAddress;
                string uri = requestHeaders[CommonTransportKeys.RequestUri] as string;
                CallContext.SetData("ClientIPAddress", ip);
                CallContext.SetData("ClientURI", uri);
                ServerProcessing spres = _nextSink.ProcessMessage(
                    sinkStack,
                    requestMsg,
                    requestHeaders,
                    requestStream,
                    out responseMsg,
                    out responseHeaders,
                    out responseStream);
                return spres;
            }
            else
            {
                responseMsg = null;
                responseHeaders = null;
                responseStream = null;
                return new ServerProcessing();
            }
        }


    }
}
