package com.aiventra.app.data.repository

import com.aiventra.app.data.model.*
import com.aiventra.app.data.remote.AiventraApiService
import com.google.firebase.firestore.FirebaseFirestore
import com.google.firebase.firestore.Query
import kotlinx.coroutines.channels.awaitClose
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.callbackFlow
import kotlinx.coroutines.tasks.await
import okhttp3.MediaType.Companion.toMediaTypeOrNull
import okhttp3.MultipartBody
import okhttp3.RequestBody.Companion.asRequestBody
import okhttp3.RequestBody.Companion.toRequestBody
import java.io.File
import javax.inject.Inject
import javax.inject.Singleton

sealed class Result<out T> {
    data class Success<T>(val data: T) : Result<T>()
    data class Error(val message: String, val cause: Throwable? = null) : Result<Nothing>()
    object Loading : Result<Nothing>()
}

@Singleton
class AiventraRepository @Inject constructor(
    private val api: AiventraApiService,
    private val db: FirebaseFirestore,
) {

    // ─── Firestore cases (cases collection — shared with web) ─────────────────

    fun observeCases(): Flow<Result<List<Case>>> = callbackFlow {
        trySend(Result.Loading)
        val listener = db.collection("cases")
            .orderBy("created_at", Query.Direction.DESCENDING)
            .limit(100)
            .addSnapshotListener { snap, err ->
                if (err != null) {
                    trySend(Result.Error("Firestore error: ${err.message}", err))
                    return@addSnapshotListener
                }
                val cases = snap?.documents?.mapNotNull { doc ->
                    runCatching {
                        val locField = doc.get("location")
                        val locationStr = when (locField) {
                            is String -> locField
                            is Map<*, *> -> (locField["address"] as? String) ?: ""
                            else -> ""
                        }
                        val createdAtStr = when (val ca = doc.get("created_at")) {
                            is String -> ca
                            is com.google.firebase.Timestamp -> ca.toDate().toString()
                            else -> ""
                        }
                        Case(
                            id = doc.id,
                            caseNumber = doc.getString("case_number") ?: "",
                            title = doc.getString("title") ?: "Untitled",
                            location = locationStr,
                            status = doc.getString("status") ?: "active",
                            priority = doc.getString("priority") ?: "medium",
                            caseType = doc.getString("case_type") ?: "homicide",
                            riskScore = (doc.getLong("risk_score") ?: 0L).toInt(),
                            riskLevel = doc.getString("risk_level") ?: "low",
                            description = doc.getString("description") ?: "",
                            evidenceCount = (doc.getLong("evidence_count") ?: 0L).toInt(),
                            createdAt = createdAtStr,
                        )
                    }.getOrNull()
                } ?: emptyList()
                trySend(Result.Success(cases))
            }
        awaitClose { listener.remove() }
    }

    suspend fun createCase(case_: Case): Result<String> {
        return try {
            val now = com.google.firebase.Timestamp.now()
            val auth = com.google.firebase.auth.FirebaseAuth.getInstance().currentUser
            val ref = db.collection("cases").add(
                hashMapOf(
                    "case_number" to case_.caseNumber,
                    "title" to case_.title,
                    "location" to case_.location,
                    "status" to "active",
                    "priority" to case_.priority,
                    "case_type" to case_.caseType,
                    "risk_score" to 0,
                    "risk_level" to "low",
                    "description" to case_.description,
                    "evidence_count" to 0,
                    "created_at" to now,
                    "updated_at" to now,
                    "created_by" to mapOf(
                        "id" to (auth?.uid ?: "anonymous"),
                        "name" to (auth?.displayName
                            ?: auth?.email?.substringBefore("@")
                            ?: "Investigator"),
                    ),
                    "platform" to "android",
                )
            ).await()
            Result.Success(ref.id)
        } catch (t: Throwable) {
            Result.Error(t.message ?: "Failed to create case", t)
        }
    }

    // ─── Autopsy PDF analysis (FastAPI) ──────────────────────────────────────

    suspend fun analyzeAutopsy(file: File, caseId: String?): Result<AutopsyResult> {
        return try {
            val body = file.asRequestBody("application/pdf".toMediaTypeOrNull())
            val part = MultipartBody.Part.createFormData("file", file.name, body)
            val caseIdBody = caseId?.toRequestBody("text/plain".toMediaTypeOrNull())
            val response = api.analyzeAutopsy(part, caseIdBody)
            if (response.isSuccessful) {
                Result.Success(response.body()!!)
            } else {
                Result.Error("Server error ${response.code()}: ${response.message()}")
            }
        } catch (t: Throwable) {
            Result.Error("Network error: ${t.message}", t)
        }
    }

    // ─── Image → body chart (FastAPI) ─────────────────────────────────────────

    suspend fun generateBodyChart(file: File, mimeType: String = "image/jpeg"): Result<ImageAnalysisResult> {
        return try {
            val body = file.asRequestBody(mimeType.toMediaTypeOrNull())
            val part = MultipartBody.Part.createFormData("file", file.name, body)
            val response = api.generateBodyChart(part)
            if (response.isSuccessful) {
                Result.Success(response.body()!!)
            } else {
                Result.Error("Server error ${response.code()}: ${response.message()}")
            }
        } catch (t: Throwable) {
            Result.Error("Network error: ${t.message}", t)
        }
    }

    // ─── AI Assistant (FastAPI) ────────────────────────────────────────────────

    suspend fun askAssistant(
        query: String,
        caseId: String? = null,
        history: List<ChatMessage> = emptyList(),
    ): Result<AssistantResponse> {
        return try {
            val req = AssistantRequest(
                query = query,
                caseId = caseId,
                history = history.takeLast(6),
            )
            val response = api.askAssistant(req)
            if (response.isSuccessful) {
                Result.Success(response.body()!!)
            } else {
                Result.Error("Server error ${response.code()}: ${response.message()}")
            }
        } catch (t: Throwable) {
            Result.Error("Network error: ${t.message}", t)
        }
    }

    // ─── Timeline (FastAPI) ───────────────────────────────────────────────────

    suspend fun getTimeline(caseId: String): Result<List<TimelineEvent>> {
        return try {
            val response = api.getTimeline(caseId)
            if (response.isSuccessful) {
                Result.Success(response.body()?.events ?: emptyList())
            } else {
                Result.Error("Server error ${response.code()}")
            }
        } catch (t: Throwable) {
            Result.Error("Network error: ${t.message}", t)
        }
    }

    // ─── Risk score (FastAPI) ─────────────────────────────────────────────────

    suspend fun getRiskScore(caseId: String): Result<RiskResult> {
        return try {
            val response = api.getRiskScore(caseId)
            if (response.isSuccessful) {
                Result.Success(response.body()!!)
            } else {
                Result.Error("Server error ${response.code()}")
            }
        } catch (t: Throwable) {
            Result.Error("Network error: ${t.message}", t)
        }
    }

    // ─── Geo markers (Firestore) ──────────────────────────────────────────────

    suspend fun getGeoMarkers(caseId: String): Result<List<GeoMarker>> {
        return try {
            val snap = db.collection("geo_markers")
                .whereEqualTo("case_id", caseId)
                .get()
                .await()
            val markers = snap.documents.mapNotNull { doc ->
                runCatching {
                    GeoMarker(
                        id = doc.id,
                        caseId = doc.getString("case_id") ?: caseId,
                        type = doc.getString("type") ?: "crime_scene",
                        lat = doc.getDouble("lat") ?: 0.0,
                        lng = doc.getDouble("lng") ?: 0.0,
                        label = doc.getString("label") ?: "",
                        note = doc.getString("note") ?: "",
                    )
                }.getOrNull()
            }.filter { it.lat != 0.0 && it.lng != 0.0 }
            Result.Success(markers)
        } catch (t: Throwable) {
            Result.Error("Firestore error: ${t.message}", t)
        }
    }
}
