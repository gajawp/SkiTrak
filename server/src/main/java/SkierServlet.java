import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;
import model.LiftRide;
import model.ResponseMsg;
import util.RequestStats;

import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.BufferedReader;
import java.io.IOException;

@WebServlet("/skiers/*")
public class SkierServlet extends HttpServlet {

    private final Gson gson = new Gson();

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse res)
            throws IOException {

        configureResponse(res);
        String urlPath = req.getPathInfo();

        if ("/stats".equals(urlPath)) {
            writeResponse(res, HttpServletResponse.SC_OK,
                    RequestStats.getStats());
            return;
        }

        RequestStats.incrementGet();

        if (urlPath == null || urlPath.isEmpty() || "/".equals(urlPath)) {
            writeResponse(res, HttpServletResponse.SC_BAD_REQUEST,
                    "missing parameters");
            return;
        }

        String[] urlParts = urlPath.split("/", -1);

        if (!isUrlValid(urlParts)) {
            writeResponse(res, HttpServletResponse.SC_BAD_REQUEST,
                    "invalid url");
            return;
        }

        String message = String.format(
                "GET request processed for skier %s at resort %s, season %s, day %s",
                urlParts[7], urlParts[1], urlParts[3], urlParts[5]);

        writeResponse(res, HttpServletResponse.SC_OK, message);
    }

    @Override
    protected void doPost(HttpServletRequest req, HttpServletResponse res)
            throws IOException {

        configureResponse(res);
        String urlPath = req.getPathInfo();

        if (urlPath == null || urlPath.isEmpty() || "/".equals(urlPath)) {
            writeResponse(res, HttpServletResponse.SC_BAD_REQUEST,
                    "missing parameters");
            return;
        }

        String[] urlParts = urlPath.split("/", -1);

        if (!isUrlValid(urlParts)) {
            writeResponse(res, HttpServletResponse.SC_BAD_REQUEST,
                    "invalid url");
            return;
        }

        try {
            StringBuilder jsonBody = new StringBuilder();

            try (BufferedReader reader = req.getReader()) {
                String line;
                while ((line = reader.readLine()) != null) {
                    jsonBody.append(line);
                }
            }

            LiftRide liftRide = gson.fromJson(
                    jsonBody.toString(), LiftRide.class);

            if (liftRide == null || !isLiftRideValid(liftRide)) {
                writeResponse(res, HttpServletResponse.SC_BAD_REQUEST,
                        "invalid JSON body: time must be 1-360 and liftID must be 1-40");
                return;
            }

            // Assignment 1 only validates and acknowledges the swipe.
            // No persistence or aggregation is required.
            writeResponse(res, HttpServletResponse.SC_CREATED,
                    "Lift ride accepted");

        } catch (JsonSyntaxException e) {
            writeResponse(res, HttpServletResponse.SC_BAD_REQUEST,
                    "invalid JSON body");
        }
    }

    private boolean isUrlValid(String[] urlParts) {
        // /{resortID}/seasons/{seasonID}/days/{dayID}/skiers/{skierID}
        if (urlParts.length != 8
                || !urlParts[0].isEmpty()
                || !"seasons".equals(urlParts[2])
                || !"days".equals(urlParts[4])
                || !"skiers".equals(urlParts[6])) {
            return false;
        }

        return isPositiveInteger(urlParts[1])
                && isPositiveInteger(urlParts[3])
                && isPositiveInteger(urlParts[5])
                && isPositiveInteger(urlParts[7]);
    }

    private boolean isPositiveInteger(String value) {
        if (value.isEmpty()) {
            return false;
        }

        for (int i = 0; i < value.length(); i++) {
            char character = value.charAt(i);
            if (character < '0' || character > '9') {
                return false;
            }
        }

        try {
            return Integer.parseInt(value) > 0;
        } catch (NumberFormatException e) {
            return false;
        }
    }

    private boolean isLiftRideValid(LiftRide liftRide) {
        return liftRide.getTime() >= 1
                && liftRide.getTime() <= 360
                && liftRide.getLiftID() >= 1
                && liftRide.getLiftID() <= 40;
    }

    private void configureResponse(HttpServletResponse res) {
        res.setContentType("application/json");
        res.setCharacterEncoding("UTF-8");
    }

    private void writeResponse(HttpServletResponse res,
                               int statusCode,
                               String message) throws IOException {
        res.setStatus(statusCode);
        res.getWriter().write(gson.toJson(new ResponseMsg(message)));
    }
}