package kotlinx.jetsocket

import com.google.gson.Gson
import rx.Observable
import rx.Subscription
import rx.subjects.PublishSubject
import rx.subjects.Subject
import java.io.InputStream
import java.nio.ByteBuffer
import java.util.ArrayList
import java.util.HashMap
import javax.servlet.ServletContextEvent
import javax.servlet.ServletContextListener
import javax.servlet.annotation.WebListener
import javax.websocket.*

public val Session.userProperties : MutableMap<String, Any>
    get() = this.getUserProperties()

public trait Request<Input> {
    val session: Session
    val input : Observable<Input>
}

class RequestImpl<Input>(override val session : Session, override val input : PublishSubject<Input>, volatile var subscription : Subscription? = null) : Request<Input>
class Pipe<Input, Output>(val request : Request<Input>, val output : Observable<Output>)

public abstract class WebSocketWriteOnly<Output>(body : (Observable<Request<kotlin.Any>>) -> Observable<Pipe<kotlin.Any, Output>>) : WebSocket<Any, Output>(javaClass<Any>(), body)

public abstract class WebSocket<Input, Output>(val inputClass : Class<Input>, val body : (Observable<Request<Input>>) -> Observable<Pipe<Input, Output>>) {
    private val subject = PublishSubject.create<Request<Input>>()
    val gson = Gson()
    val subscription = body(subject).subscribe { pipe ->
        val request : RequestImpl<Input>? = when (pipe.request) {
            is RequestImpl<Input> -> pipe.request
            else -> getRequestFromSession(pipe.request.session)
        }

        if (request == null) {
            return@subscribe closeAndUnSubscribe(pipe.request.session)
        }

        request.subscription = pipe.output.doOnCompleted {
            closeAndUnSubscribe(request.session)
        }.subscribe {
            request.session.getBasicRemote().sendText(gson.toJson(it))
        }

        if (!pipe.request.session.isOpen()) {
            request.subscription?.unsubscribe()
        }
    }

    [OnOpen]
    final fun opened(session : Session) {
        val eventsSubject = PublishSubject.create<Input>()
        val request = RequestImpl(session, eventsSubject)
        putRequestToSession(session, request)

        subject.onNext(request)

        if (!subject.hasObservers()) {
            session.close(CloseReason(CloseReason.CloseCodes.CANNOT_ACCEPT, "web socket couldn't accept connections because no observers registered in ${this.javaClass.getName()}"))
        }
    }

    [OnClose]
    final fun closed(session : Session) {
        closeAndUnSubscribe(session)
    }

    [OnMessage]
    final fun received(session : Session, message : String) {
        val request = getRequestFromSession(session)

        if (request == null) {
            return closeAndUnSubscribe(session, CloseReason(CloseReason.CloseCodes.TRY_AGAIN_LATER, "Server probably not yet ready"))
        }

        try {
            request.input.onNext(gson.fromJson(message, inputClass))
        } catch(e : Throwable) {
            request.input.onError(e)
        }
    }

    [OnMessage]
    final fun received(session : Session, input : InputStream) {
        val request = getRequestFromSession(session)

        if (request == null) {
            return closeAndUnSubscribe(session, CloseReason(CloseReason.CloseCodes.TRY_AGAIN_LATER, "Server probably not yet ready"))
        }

        try {
            request.input.onNext(gson.fromJson(input.reader(Charsets.UTF_8), inputClass))
        } catch(e : Throwable) {
            request.input.onError(e)
        }
    }

    val emptyBB = ByteBuffer.allocate(0)
    [OnMessage]
    final fun ping(session : Session, ping : PongMessage) {
        println("Ping received");
        session.getBasicRemote().sendPong(emptyBB)
    }

    private fun closeAndUnSubscribe(session : Session, reason : CloseReason = CloseReason(CloseReason.CloseCodes.NORMAL_CLOSURE, "")) {
        val request = getRequestFromSession(session)

        request?.subscription?.unsubscribe()
        request?.input?.onCompleted()
        request?.subscription?.unsubscribe()

        try {
            session.close(reason)
        } catch(ignore : Throwable) {
        }
    }

    [suppress("UNCHECKED_CAST")]
    private fun getRequestFromSession(session: Session) = session.userProperties["r"] as RequestImpl<Input>?
    private fun putRequestToSession(session : Session, request : Request<*>) {
        session.userProperties["r"] = request
    }

}

public abstract class ServletContextListener : ServletContextListener {
    override fun contextInitialized(sce: ServletContextEvent?) {
    }

    override fun contextDestroyed(sce: ServletContextEvent?) {
    }
}